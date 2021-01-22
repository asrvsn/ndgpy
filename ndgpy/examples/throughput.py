''' Simple end-to-end examples of process graphs ''' 

from typing import Callable
import numpy as np
import asyncio

from ndgpy.utils import now
from ndgpy.nodes.numeric import *

class Noise(Emitter):
	def __init__(self, delta=1.0):
		self.delta = delta
		super().__init__(np.dtype([('f0', np.float64)]))

	async def compute(self):
		await asyncio.sleep(self.delta)
		self.output['f0'] = np.random.uniform()
		# print('Noise:', self.output['f0'])

class Clock(Emitter):
	def __init__(self, delta=1.0):
		self.delta = delta
		super().__init__(np.dtype([('f0', np.float64)]))

	async def compute(self):
		await asyncio.sleep(self.delta)
		print('Clock:', now())

class Throughput(Collector, Resourced):
	async def start(self, res):
		self.start_time = now()
		self.ctr = 0

	async def stop(self):
		delta = (now() - self.start_time) / np.timedelta64(1, 's')
		print(f'Throughput: {self.ctr/delta} / sec')

	async def compute(self, values):
		self.ctr += 1

# Example

class TestLayout(Layout):
	async def setup(self):
		ctx1 = self.new_context()

		# p1 = Clock()
		p1 = Noise(delta=0.0)
		await self.add(p1, ctx1)

		p2 = Noise(delta=0.0)
		await self.add(p2, ctx1)

		ctx2 = self.new_context()

		p3 = Lambda(lambda x, y: x * y)
		await self.add(p3, ctx2)
		await self.connect(p1.id, p3.id)
		await self.connect(p2.id, p3.id)

		ctx3 = self.new_context()

		p4 = Throughput()
		await self.add(p4, ctx3)
		await self.connect(p3.id, p4.id)

if __name__ == '__main__':
	TestLayout().start()