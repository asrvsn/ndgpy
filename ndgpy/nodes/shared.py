''' Nodes with shared output values ''' 

from ndgpy.data import *
from .base import *
from .interfaces import *

class SharedEmitter(Emitter, Resourced):
	def __init__(self, dtype: np.dtype, smm: AFSharedMemoryManager, id: NodeID=None, rspec: ResourceSpec=set()):
		Emitter.__init__(self, dtype, id=id)
		rspec.add(Resource.smm)
		Resourced.__init__(self, rspec)
		self.output = SharedStruct.from_pure(smm, self.output)
		self.output_serialized = self.output.serialize()

	async def start(self, res: Resources):
		super().start(res)
		self.output = deserialize_data(res[Resource.smm], self.output_serialized)


class SharedRouter(Router, SharedEmitter):
	def __init__(self, dtype: np.dtype, smm: AFSharedMemoryManager, id: NodeID=None, rspec: ResourceSpec=set()):
		SharedEmitter.__init__(self, dtype, smm, id=id, rspec=rspec)
		Collector.__init__(self, id=self.id)