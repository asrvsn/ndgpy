''' Complete mechanism for runnning Process graphs along with boundary I/O ''' 

import shortuuid
from typing import NewType, List, Callable, Tuple, Dict, Any, Set
from abc import ABC, abstractmethod
import zmq
import zmq.asyncio as azmq
import aiozmq
import multiprocessing as mp
import ujson
import asyncio
import pdb
import sys

from .utils import *
from .data.shared import *
from .nodes.base import *
from .nodes.boundary import *
from .context import *
from .network import *


''' Common types ''' 

Publication = Tuple[NodeID, SharedData]
Subscription = Tuple[NodeID, ContextID]

''' Orchestrator class / entry point '''

class Orchestrator(ABC):
	''' Graph execution runner / manager for processes across multiple execution contexts
	For now, restricted to single machine.

	Duties:
	* expose an orchestrator API 
	''' 
	def __init__(self):
		''' Subclasses should in general override setup() rather than __init__() '''
		self.contexts: Dict[ContextID, Context] = {}
		self.nodes: Dict[NodeID, Node] = {}
		self.addrs: Dict[NodeID, ContextID] = {}
		self.publications: Dict[NodeID, Publication] = {}	# Tracking published nodes (key is publisher ID)
		self.subscriptions: Dict[Subscription, NodeID] = {}	# Tracking subscribed nodes (key contains publisher ID)

	def __enter__(self):
		# Initialize sockets & shared mem
		self.zmq_ctx = azmq.Context()
		self.tx = self.zmq_ctx.socket(zmq.PUB)
		self.tx.bind(tx_url)
		self.rx = self.zmq_ctx.socket(zmq.PULL)
		self.rx.bind(rx_url)
		self.smm = AFSharedMemoryManager().__enter__()
		return self

	def __exit__(self, exc_type, exc_value, tb):
		# Destroy sockets & running nodes (should implicitly destroy contexts due to OS management)
		self.zmq_ctx.destroy()
		self.smm.__exit__(exc_type, exc_value, tb)
		return exc_type is None

	''' Public methods ''' 

	# TODO: convert to private method?
	def new_context(self) -> ContextID:
		''' Allocates a new execution context ''' 
		ctx = Context()
		self.contexts[ctx.id] = ctx
		ctx.proc.start()
		return ctx.id

	def get_avail_cpus(self) -> int:
		# TODO: return n cpus based on how many context workers in use
		pass

	async def clear_context(self, ctx_id: ContextID):
		assert ctx_id in self.contexts
		await asyncio.wait([self.remove(n) for n in self.nodes.keys() if self.addrs[n] == ctx_id])

	# TODO: convert to private method?
	async def destroy_context(self, ctx_id: ContextID): 
		# TODO: fix this. context should be told to destroy self, not preemptively stopped. (use signals?)
		await self.clear_context(ctx_id)
		self.contexts[ctx_id].stop()
		del self.contexts[ctx_id]

	# TODO: convert to private method?
	async def add(self, node: Node, ctx_id: ContextID):
		''' Adds a node to the appropriate execution context & starts running it ''' 
		assert ctx_id in self.contexts
		assert node.id not in self.nodes, 'Cannot add an already added node' # TODO: convert to warning?
		self.nodes[node.id] = node
		self.addrs[node.id] = ctx_id
		await self.notify(ctx_id, {'add': wire_pickle(node)})

	async def remove(self, n_id: NodeID):
		assert n_id in self.nodes
		# First disconnect all neighbors (will automatically un-publish if necessary)
		await asyncio.wait(
			[self.disconnect(neighbor, n_id) for neighbor in self.nodes[n_id].sources] + 
			[self.disconnect(neighbor, n_id) for neighbor in self.nodes[n_id].sinks]
		)
		await self.notify(self.addrs[n_id], {'remove': n_id})
		del self.nodes[n_id]
		del self.addrs[n_id]

	# TODO: convert to private method?
	async def connect(self, n_id_1: NodeID, n_id_2: NodeID, buffer_size=None):
		''' Connects two nodes which are runnning. Double-calls are idempotent ''' 
		assert all((n_id_1, n_id_2 in self.nodes))
		self.nodes[n_id_1].sends_to(self.nodes[n_id_2]) # Store link locally
		ctx1, ctx2 = self.addrs[n_id_1], self.addrs[n_id_2]
		if ctx1 == ctx2:
			# The nodes are running on the same execution context
			await self.notify(ctx1, {'connect': {'parent': n_id_1, 'child': n_id_2}})
		else:
			# The nodes are running on different execution contexts; establish a shared memory link
			await self.link(n_id_1, n_id_2, buffer_size=buffer_size)

	async def create_edges(self, node: Node):
		''' Create edges to other nodes which are already present. '''
		tasks = []
		if isinstance(node, Emitter):
			tasks += [self.connect(node.id, n.id) for n in node.sinks if n.id in self.nodes]
		if isinstance(node, Collector):
			tasks += [self.connect(n.id, node.id) for n in node.sources if n.id in self.nodes]
		await asyncio.wait(tasks)

	# TODO: convert to private method?
	async def disconnect(self, n_id_1: NodeID, n_id_2: NodeID):
		''' n_id_1 is parent, n_id_2 is child '''
		assert all((n_id_1, n_id_2 in self.nodes))
		self.nodes[n_id_1].disconnect(self.nodes[n_id_2]) # Update link locally
		ctx1, ctx2 = self.addrs[n_id_1], self.addrs[n_id_2]
		if ctx1 == ctx2:
			# The nodes are running on the same execution context
			await self.notify(ctx1, {'disconnect': {'parent': n_id_1, 'child': n_id_2}})
		else:
			await self.unlink(n_id_1, n_id_2)

	# TODO: convert to private method?
	async def parameterize(self, n_id_1: NodeID, n_id_2: NodeID): 
		# TODO check double-calls in this method
		assert all((n_id_1, n_id_2 in self.nodes))
		assert isinstance(self.nodes[n_id_2], Parametrized), 'Node 2 is not parameterizable'
		link = self.nodes[n_id_2].parameters_serialized
		writer = Writer(link, mode=WriteMode.merge)
		await self.add(writer, self.addrs[n_id_1])
		await self.connect(n_id_1, writer.id)
		
	@abstractmethod
	async def setup(self):
		''' Subclasses should define initial set-up of computation graph. Method run once after resources are intiialized. ''' 
		pass

	async def run(self):
		''' Method run after self.setup(). Any (asynchronous) long-running ops go in this method. ''' 
		pass

	def start(self):
		try:
			asyncio.run(self.main())
		except KeyboardInterrupt:
			sys.exit(1)

	''' Private methods ''' 

	async def link(self, n_id_1: NodeID, n_id_2: NodeID, buffer_size=None):
		''' Like self.connect(), but across execution contexts ''' 
		# Establish the publisher
		assert all((n_id_1, n_id_2 in self.nodes))
		if n_id_1 not in self.publications:
			node = self.nodes[n_id_1]
			if buffer_size is None: # Link is not buffered
				data = SharedStruct(self.smm, node.dtype)
			else:
				assert buffer_size > 0
				data = SharedStreamingArray(self.smm, node.dtype, buffer_size)
			# Add publisher process attached to source
			pub = Publisher(n_id_1, data.serialize())
			await self.add(pub, self.addrs[n_id_1])
			await self.connect(n_id_1, pub.id)
			self.publications[n_id_1] = (pub.id, data)

		# Establish the subscriber
		ctx_id = self.addrs[n_id_2]
		# Do we already have a subscription in that context, then connect to it
		sub_key = (n_id_1, ctx_id)
		if sub_key in self.subscriptions: 
			await self.connect(self.subscriptions[sub_key], n_id_2)
		# Else create a subscription
		else:
			sub = Subscriber(n_id_1, self.publications[n_id_1][1].serialize())
			await self.add(sub, self.addrs[n_id_2])
			await self.connect(sub.id, n_id_2)
			self.subscriptions[sub_key] = sub.id

	async def unlink(self, n_id_1: NodeID, n_id_2: NodeID):
		assert all((n_id_1, n_id_2 in self.nodes))
		assert n_id_1 in self.publications
		pub_id = self.publications[n_id_1][0]
		sub_id = self.subscriptions[(n_id_1, self.addrs[n_id_2])]
		await self.disconnect(sub_id, n_id_2)
		if len(self.nodes[sub_id].sinks) == 0:
		# Subscriber has no children, get rid of it
			await self.remove(sub_id)
			del self.subscriptions[(n_id_1, self.addrs[n_id_2])]
		if not any(sub[0] == n_id_1 for sub in self.subscriptions):
		# Publisher has no subscribers, get rid of it
			await self.remove(pub_id)
			del self.publications[n_id_1]


	async def notify(self, ctx_id: ContextID, msg: Dict):
		await self.contexts[ctx_id].ready.wait() # Wait for context to be ready
		await self.tx.send_string(ctx_id + ' ' + ujson.dumps(msg))

	async def recv_loop(self):
		while True:
			msg = await self.rx.recv_json()
			# print(f'Host got message: {msg}')
			if 'ready' in msg:
				self.contexts[msg['ready']].ready.set() # Set worker readiness

	async def main(self):
		''' Call when nodes have been initialized from the script (nodes may continue to be added by messaging the host)
		Blocks until interrupted. Can use this method alongside other coroutines.
		'''
		with self as self:
			print('Orchestrator started')
			async def run():
				await self.setup()
				await self.run()
			await asyncio.gather(
				self.recv_loop(), 
				run(),
			)
