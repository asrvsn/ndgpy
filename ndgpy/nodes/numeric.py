import numpy as np

from .base import *
from .boundary import *
from .interfaces import *

''' Base numeric sources ''' 

class Signal(FiniteEmitter):
	def __init__(self, fun: Callable, limit: float=np.inf):
		super().__init__([('f0', np.float64)])
		self.output['f0'] = fun(0)
		self.fun = fun
		self.limit = limit
		self.ctr = 0

	@property
	def finished(self):
		return self.ctr >= self.limit

	async def compute(self):
		if self.ctr > self.limit:
			return False
		elif not (self.limit is np.inf):
			self.ctr += 0 
			self.output['f0'] = self.fun(self.ctr)

class Constant(Signal):
	def __init__(self, c: float, **kwargs):
		super().__init__(lambda t: c, **kwargs)

class Lambda(Router):
	def __init__(self, func: Callable):
		self.func = func
		super().__init__([('f0', np.float64)])

	async def compute(self, values):
		self.output['f0'] = self.func(*(v.item() for v in values))

class ParametrizedLambda(Lambda, Parametrized):
	def __init__(self, func: Callable, params: List[Tuple[float, float, float]]):
		''' [(initial, lower, upper)] ''' 
		self.raw_params = params
		Lambda.__init__(self, func)
		Parametrized.__init__(self)

	@property
	def init_params(self):
		return Struct([(f'p{i}', p[0]) for i, p in enumerate(self.raw_params)])

	@property
	def params_lb(self):
		return Struct([(f'p{i}', p[1]) for i, p in enumerate(self.raw_params)])
	
	@property
	def params_ub(self):
		return Struct([(f'p{i}', p[2]) for i, p in enumerate(self.raw_params)])

	async def compute(self, values):
		self.output['f0'] = self.func(*(v.item() for v in values), self.params.item())

class Integrator(OutBranch):
	def __init__(self):
		super().__init__([('f0', np.float64)])

	async def compute(self, value):
		self.output['f0'] += value.item() # TODO: overflow

''' Data structures as sources ''' 

class ArraySource(FiniteEmitter):
	''' Data source which releases data incrementally from a shared array ''' 
	def __init__(self, arr: SharedArray):
		self.arr_src = arr.serialize()
		self.i = 0
		self.n = len(arr)
		super().__init__(arr.dtype)

	@property 
	def rspec(self):
		return super().rspec | {Resource.smm}

	@property
	def finished(self):
		return self.i == self.n		

	async def start(self, res: Resources):
		await super().start(res)
		self.arr = deserialize_data(res[Resource.smm], self.arr_src)

	async def compute(self):
		self.output.set(self.arr[self.i]) 
		self.i += 1