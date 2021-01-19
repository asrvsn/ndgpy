''' Base node types ''' 

import shortuuid
import numpy as np
from typing import NewType, List, Callable, Tuple, Dict, Any, Set
from abc import ABC, abstractmethod
import zmq
import zmq.asyncio
import multiprocessing as mp
import asyncio

from ndgpy.data import *
from ndgpy.utils import *


''' Common types ''' 

NodeID = NewType('NodeID', str)

''' Base node definitions ''' 

class Emitter(ABC):
	''' Object which emits data and can be run in a loop without parents ''' 
	def __init__(self, dtype: np.dtype, id: NodeID=None):
		self.id = shortuuid.uuid() if id is None else id
		self.output = Struct(dtype) # Output type
		self.sinks: Dict[NodeID, Collector] = dict()

	async def __call__(self):
		result = await self.compute()
		# await asyncio.sleep(0) # Runs a BFS 
		if result is not False: # Optional propagation
			await asyncio.gather(*(proc(self.id) for proc in self.sinks.values()))

	def sends_to(self, *procs: Tuple['Collector']): 
		''' An Emitter can send data to Processes ''' 
		for proc in procs:
			self.sinks[proc.id] = proc
			if self.id not in proc.sources:
				proc.receives_from(self)

	def disconnect(self, *procs: Tuple['Collector']):
		for proc in procs:
			if proc.id in self.sinks:
				del self.sinks[proc.id]
				proc.disconnect(self)

	@abstractmethod
	async def compute(self):
		''' Emit the data. 
		It is the subclass's responsibility to update `self.output` in this method.
		Can update state, fill buffers, etc. Only asynchronous I/O should be used.
		Returning `False` from this method will prevent triggering children.
		'''
		pass

	@property
	def dtype(self):
		return self.output.dtype

class Collector(ABC):
	''' Object which collects / drains data (typically, with I/O) ''' 
	def __init__(self, id: NodeID=None):
		self.id = shortuuid.uuid() if id is None else id
		self.sources: Dict[NodeID, Emitter] = dict()
		self.flags: Dict[NodeID, bool] = dict() # Completion flags

	async def __call__(self, *inputs: Tuple[NodeID]):
		''' Completion trigger from other nodes ''' 
		for pid in inputs:
			assert pid in self.sources
			self.flags[pid] = True

		if self.is_ready(): 
			await self.compute(tuple(self.sources[pid].output for pid in inputs))
			self.reset_flags()

	def is_ready(self):
		return all(self.flags.values())

	def reset_flags(self):
		''' Reset completion flags '''
		for k in self.sources:
			self.flags[k] = False

	def receives_from(self, *procs: Tuple['Emitter']):
		''' A Collector can receive from Emitters & Routers ''' 
		for proc in procs:
			assert proc != self
			self.sources[proc.id] = proc
			self.flags[proc.id] = False
			if self.id not in proc.sinks:
				proc.sends_to(self)

	def disconnect(self, *procs: Tuple['Emitter']):
		for proc in procs:
			if proc.id in self.sources:
				del self.sources[proc.id]
				proc.disconnect(self)

	@abstractmethod
	async def compute(self, values: Tuple[Struct]):
		''' Computation when all dependents have updated. 
		Arguments will be passed in the order that connections were created to parent nodes.
		Can update state, fill buffers, etc. Only asynchronous I/O should be used.
		'''
		pass

class Router(Emitter, Collector):
	''' A node which can do both Collecting (receiving) and Emitting (transmitting) ''' 
	def __init__(self, dtype: np.dtype, id: NodeID=None):
		Emitter.__init__(self, dtype, id=id)
		Collector.__init__(self, id=self.id)

	async def __call__(self, *inputs: Tuple[NodeID]): 
		''' Completion trigger from other processes ''' 
		for pid in inputs:
			assert pid in self.sources
			self.flags[pid] = True

		# Advance to sink nodes if sources have completed 
		if self.is_ready(): 
			result = await self.compute(tuple(p.output for p in self.sources.values()))
			self.reset_flags()
			# await asyncio.sleep(0) # Runs a BFS 
			if result is not False: # Optional propagation
				await asyncio.gather(*(proc(self.id) for proc in self.sinks.values()))

	def disconnect(self, *procs: Tuple['Router']):
		Emitter.disconnect(self, *procs)
		Collector.disconnect(self, *procs)

Node = Union[Emitter, Collector, Router]

''' Derived topologies ''' 

class SingleEmitter(Emitter):
	''' Single-sink emitter ''' 
	def sends_to(self, proc: 'Collector'):
		assert self.sinks == {}, 'SingleEmitter already has a sink'
		super().sends_to(proc)

	async def __call__(self):
		result = await self.compute()
		# await asyncio.sleep(0) # Runs a BFS 
		if result is not False and len(self.sinks) != 0: # Optional propagation
			await self.sinks[next(iter(self.sinks))](self.id)

class SingleCollector(Collector):
	''' Single-source collector ''' 
	def receives_from(self, proc: 'Emitter'):
		assert self.sources == {}, 'SingleCollector already has a source'
		super().receives_from(proc)

	async def __call__(self, src_id: NodeID):
		assert src_id in self.sources
		await self.compute(self.sources[src_id].output)

	@abstractmethod
	async def compute(self, value: Struct):
		pass

class Pipe(SingleEmitter, SingleCollector):
	''' Source-to-sink pipe connector ''' 
	def __init__(self, dtype: np.dtype, id: NodeID=None):
		SingleEmitter.__init__(self, dtype, id=id)
		SingleCollector.__init__(self, id=self.id)

	async def __call__(self, src_id: NodeID):
		result = await self.compute(self.sources[src_id].output)
		# await asyncio.sleep(0) # Runs a BFS 
		if result is not False and len(self.sinks) != 0:
			await self.sinks[next(iter(self.sinks))](self.id)

class OutBranch(Emitter, SingleCollector):
	''' Single-source many-sink connector ''' 
	def __init__(self, dtype: np.dtype, id: NodeID=None):
		Emitter.__init__(self, dtype, id=id)
		SingleCollector.__init__(self, id=self.id)

	async def __call__(self, src_id: NodeID):
		result = await self.compute(self.sources[src_id].output)
		# await asyncio.sleep(0) # Runs a BFS 
		if result is not False: # Optional propagation
			await asyncio.gather(*(proc(self.id) for proc in self.sinks.values()))

class InBranch(SingleEmitter, Collector):
	''' Many-source single-sink connector ''' 
	def __init__(self, dtype: np.dtype, id: NodeID=None):
		SingleEmitter.__init__(self, dtype, id=id)
		Collector.__init__(self, id=self.id)

	async def __call__(self, *inputs: Tuple[NodeID]): 
		''' Completion trigger from other processes ''' 
		for pid in inputs:
			assert pid in self.sources
			self.flags[pid] = True

		# Advance to sink nodes if sources have completed 
		if self.is_ready(): 
			result = await self.compute(tuple(p.output for p in self.sources.values()))
			self.reset_flags()
			# await asyncio.sleep(0) # Runs a BFS 
			if result is not False and len(self.sinks) != 0: # Optional propagation
				await self.sinks[next(iter(self.sinks))](self.id)
