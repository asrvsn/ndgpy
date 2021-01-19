''' Execution contexts ''' 

import shortuuid
from typing import NewType, List, Callable, Tuple, Dict, Any, Set
import zmq
import zmq.asyncio as azmq
import multiprocessing as mp
import ujson
import asyncio
import pdb
import sys

from .utils import *
from .data.shared import *
from .nodes.base import *
from .nodes.interfaces import *
from .network import *

''' Common types ''' 
ContextID = NewType('ContextID', str)

''' Context workers ''' 

class ContextWorker:
	''' Manages execution of a single Context
	Duties: 
	* Receive basic things (locks, etc.) (+ create basic class if needed)
	* Start memory management
	* Establish IPC w/ host process to receive updates
	* Initially (when there are 0 nodes to run), simply wait for messages from host
	* When nodes are received to run, link them appropriately & use the relevant runner (while continuing to listen for host updates)
	''' 
	def __init__(self, id: ContextID):
		self.id = id
		self.nodes: Dict[NodeID, Node] = {}
		self.emitters: Dict[NodeID, asyncio.Task] = {} # Root nodes in the graph associated with their workers

	async def __aenter__(self):
		self.zmq_ctx = azmq.Context()
		self.rx = self.zmq_ctx.socket(zmq.SUB)
		self.rx.connect(tx_url)
		self.rx.setsockopt_string(zmq.SUBSCRIBE, str(self.id))
		self.tx = self.zmq_ctx.socket(zmq.PUSH)
		self.tx.connect(rx_url)
		self.smm = AFSharedMemoryManager().__enter__()
		self.ready = asyncio.Event()
		self.resource_map = {
			Resource.zmq_ctx: self.zmq_ctx,
			Resource.mc_url: mc_url_base,
			Resource.orch_tx_url: tx_url,
			Resource.orch_rx_url: rx_url,
			Resource.smm: self.smm,
		}
		return self

	async def __aexit__(self, exc_type, exc_value, tb):
		await asyncio.wait([
			node.stop() for node in self.nodes.values()
			if isinstance(node, Resourced)
		])
		for task in self.emitters.values():
			task.cancel()
		self.zmq_ctx.destroy()
		self.smm.__exit__(exc_type, exc_value, tb)
		return exc_type is None

	def get_resources(self, rspec: ResourceSpec):
		return {r: self.resource_map[r] for r in rspec}

	async def add(self, node: Node):
		assert node.id not in self.nodes
		if isinstance(node, Resourced):
			# Initialize with resources
			await node.start(self.get_resources(node.rspec)) 
		self.nodes[node.id] = node
		if isinstance(node, Emitter) and not isinstance(node, Router): 
			# We have an Emitter node; start a Task for it
			self.emitters[node.id] = asyncio.create_task(node(), name=node.id)
			if not self.ready.is_set():
				self.ready.set()

	async def remove(self, node: NodeID):
		assert node in self.nodes
		# First disconnect all neighbors 
		asyncio.wait([self.disconnect(n, node) for n in self.nodes[node].sources])
		asyncio.wait([self.disconnect(node, n) for neighbor in self.nodes[node].sinks])

		if node in self.emitters:
			self.emitters[node].cancel()
			if len(self.emitters) == 1:
				self.ready.clear() # Stop running context
			del self.emitters[node]
		node.stop() # Kill resources
		del self.nodes[node]

	async def connect(self, n_id_1: NodeID, n_id_2: NodeID):
		assert all((n_id_1, n_id_2 in self.nodes))
		self.nodes[n_id_1].sends_to(self.nodes[n_id_2])

	async def disconnect(self, n_id_1: NodeID, n_id_2: NodeID):
		assert all((n_id_1, n_id_2 in self.nodes))
		self.nodes[n_id_1].disconnect(self.nodes[n_id_2])

	async def recv_loop(self):
		while True:
			msg = await self.rx.recv()
			msg = ujson.loads(msg[len(self.id)+1:])
			# print(f'Context {self.id} got message: {msg}')
			if 'add' in msg:
				await self.add(wire_unpickle(msg['add']))
			if 'remove' in msg:
				await self.remove(msg['remove'])
			if 'connect' in msg:
				await self.connect(msg['connect']['parent'], msg['connect']['child'])

	async def exec_loop(self):
		while True:
			await self.ready.wait() # TODO: can remove this and use dummy task
			done, pending = await asyncio.wait(self.emitters.values(), return_when=asyncio.FIRST_COMPLETED)
			for task in done:
				done_id = task.get_name()
				self.emitters[done_id] = asyncio.create_task(self.nodes[done_id](), name=done_id)

	async def main(self):
		async with self as self:
			await self.tx.send_json({'ready': self.id}) # Signal readiness to host
			print(f'Worker {self.id} started')
			await asyncio.gather(self.exec_loop(), self.recv_loop())

def ctx_worker(ctx_id: ContextID):
	try:
		worker = ContextWorker(ctx_id)
		asyncio.run(worker.main())
	except KeyboardInterrupt:
		sys.exit(1)

class Context:
	def __init__(self, ctx_id: ContextID=None):
		self.id = shortuuid.uuid() if ctx_id is None else ctx_id
		self.proc = mp.Process(target=ctx_worker, args=(self.id,))
		self.ready = asyncio.Event()