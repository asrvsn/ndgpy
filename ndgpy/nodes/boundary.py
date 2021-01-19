''' Interactions across execution boundaries ''' 

import zmq
import zmq.asyncio as azmq
from enum import Enum

from ndgpy.data.shared import *
from .base import *
from .interfaces import *

class WriteMode(Enum):
	fill = 1
	merge = 2

class Writer(SingleCollector, Resourced):
	''' A Writer writes input data to a shared location ''' 
	def __init__(self, link: SerializedData, mode: WriteMode=WriteMode.fill):
		self.link = link
		self.mode = mode
		Collector.__init__(self)

	@property 
	def rspec(self):
		return {Resource.smm}

	async def start(self, res: Resources):
		await Resourced.start(self, res)
		self.link_data = deserialize_data(res[Resource.smm], self.link)
		self.buffered = type(self.link_data) == SharedStreamingArray

	async def compute(self, value: Struct):
		# TODO cleanup this interface
		if self.buffered:
			self.link_data.consume(value)
		else:
			if self.mode == WriteMode.fill:
				self.link_data.set(value)
			elif self.mode == WriteMode.merge:
				self.link_data.merge(value)

class Publisher(Writer): 
	''' A Publisher is a Writer that also sends a signal ''' 
	def __init__(self, source_id: NodeID, link: SerializedData, emit_every: int=1):
		''' Publisher can emit every n writes ''' 
		self.source_id = source_id
		self.emit_every = emit_every
		super().__init__(link)

	@property 
	def rspec(self):
		return super().rspec | {Resource.mc_url, Resource.zmq_ctx}

	async def start(self, res: Resources):
		await super().start(res)
		self.sock = res[Resource.zmq_ctx].socket(zmq.PUB)
		# Many-to-many multicasting uses one PUB addr per published node (beware scaling)
		self.sock.bind(res[Resource.mc_url] + self.source_id)
		self.n_writes = 0

	async def compute(self, value: Struct):
		self.n_writes += 1
		if self.n_writes == self.emit_every:
			await super().compute(value)
			await self.sock.send(b'')
			self.n_writes = 0

class Subscriber(Emitter, Resourced):
	''' Subscriber matches with publisher for listening to data across contexts ''' 
	def __init__(self, source_id: NodeID, link: SerializedData):
		self.source_id = source_id
		self.link = link
		dtype = get_ser_dtype(link)
		super().__init__(dtype)

	@property 
	def rspec(self):
		return {Resource.mc_url, Resource.zmq_ctx, Resource.smm}

	async def start(self, res: Resources):
		await Resourced.start(self, res)
		self.link_data = deserialize_data(res[Resource.smm], self.link)
		self.buffered = type(self.link_data) == SharedStreamingArray
		self.sock = res[Resource.zmq_ctx].socket(zmq.SUB)
		self.sock.connect(res[Resource.mc_url] + self.source_id)
		self.sock.setsockopt(zmq.SUBSCRIBE, b'')

	async def compute(self):
		await self.sock.recv() # Any message counts as a computation trigger
		if self.buffered:
			# TODO support buffered return?
			self.output.set(self.link_data[0])
		else:
			self.output.set(self.link_data)

class Trigger(Collector):
	''' Triggers an event upon particular state. The event should only be used within the same execution context. ''' 
	def __init__(self, *args, **kwargs):
		self.event = asyncio.Event()
		super().__init__(*args, **kwargs)

	@abstractmethod
	def trigger(self):
		''' Returns True depending on state ''' 
		pass

	async def __call__(self, *args):
		super().__call__(*args)
		if self.trigger() == True:
			self.event.set() # Wake up listening coroutines
			self.event.clear()

class FiniteEmitter(Emitter, Resourced):
	''' Emitter with fixed-term outputs; allows listeners on finish event ''' 
	@property
	@abstractmethod
	def finished(self):
		pass

	@property 
	def rspec(self):
		return {Resource.zmq_ctx}

	async def start(self, res: Resources):
		await Resourced.start(self, res)
		self.sock = res[Resource.zmq_ctx].socket(zmq.PUB)
		self.sock.bind(self.id) # For listeners

	async def listen(self, zmq_ctx):
		sock = zmq_ctx.socket(zmq.SUB)
		sock.connect(self.id)
		sock.setsockopt(zmq.SUBSCRIBE, b'')
		await sock.recv()

	async def __call__(self):
		result = await self.compute()
		# await asyncio.sleep(0) # Runs a BFS 
		if self.finished:
			await self.sock.send(b'')
		elif result is not False: # Optional propagation
			await asyncio.gather(*(proc(self.id) for proc in self.sinks.values()))
			
