''' Shared-memory data structures ''' 

from multiprocessing.shared_memory import SharedMemory, ShareableList
from multiprocessing.managers import SharedMemoryManager, dispatch
from typing import Tuple, NewType, Union

from ndgpy.utils import *
from .streaming import *
from .base import *

''' Shared memory management ''' 

class AFSharedMemoryManager(SharedMemoryManager):
	''' Allows tracking shared memory from both server/client perspectives ''' 
	def __init__(self, *args, **kwargs):
		self.client_shms = dict()
		super().__init__(*args, **kwargs)

	def SharedMemory(self, name: str=None, size: int=None):
		assert name is not None or size is not None
		with self._Client(self._address, authkey=self._authkey) as conn:
			if name is not None:
				shm = SharedMemory(name=name)
				self.client_shms[name] = shm
			else:
				shm = SharedMemory(None, create=True, size=size)
				try:
					dispatch(conn, None, 'track_segment', (shm.name,))
				except BaseException as e:
					shm.unlink()
					raise e
		return shm

	def __exit__(self, *args):
		for shm in self.client_shms.values():
			shm.close()
		super().__exit__(*args)

''' Shared data structures ''' 

ShmName = NewType('ShmName', str)
SerializedStruct = Tuple[np.dtype, ShmName]
SerializedArray = Tuple[np.dtype, int, ShmName, SerializedStruct]
SerializedData = Union[SerializedStruct, SerializedArray]

SharedArray = np.ndarray
def SharedArray(smm: AFSharedMemoryManager, arr: np.ndarray):
	# TODO
	pass

class SharedStreamingArray(StreamingArray):
	''' Shared-memory streaming array ''' 
	def __init__(self, smm: AFSharedMemoryManager, dtype: np.dtype, buf_size: int, shm_name: str=None, metadata=None):
		self.buf_size = buf_size
		self.size = buf_size*2
		if shm_name is None:
			''' To be used by server process ''' 
			tmp = np.full(self.size, np.nan, dtype=dtype)
			self.shm = smm.SharedMemory(size=tmp.nbytes)
			self.data = np.ndarray(tmp.shape, dtype=dtype, buffer=self.shm.buf)
			self.data[:] = tmp[:]
			self.metadata = SharedStruct(smm, dtype=[('head_index', np.int64)])
			self.metadata['head_index'] = self.size-1
		else:
			''' To be used by client processes '''
			assert metadata is not None
			self.shm = smm.SharedMemory(name=shm_name)
			self.data = np.ndarray(self.size, dtype=dtype, buffer=self.shm.buf)
			self.metadata = metadata

	@property
	def head_index(self):
		return self.metadata['head_index']

	@head_index.setter
	def head_index(self, val):
		self.metadata['head_index'] = val

	def serialize(self) -> SerializedArray:
		''' Send to another process ''' 
		return (wire_pickle(self.dtype), self.buf_size, self.shm.name, self.metadata.serialize())

	@staticmethod
	def deserialize(smm: AFSharedMemoryManager, arg: SerializedArray) -> 'SharedStreamingArray':
		''' Receive from another process ''' 
		return SharedStreamingArray(
			smm, wire_unpickle(arg[0]), arg[1], shm_name=arg[2], metadata=SharedStruct.deserialize(smm, arg[3])
		)


class SharedStreamingDataFrame(StreamingDataFrame):
	pass

class SharedStruct(Struct):
	def __init__(self, smm: AFSharedMemoryManager, dtype: np.dtype, shm_name: str=None):
		# self.version = 0
		if shm_name is None:
			''' To be used by server process ''' 
			tmp = np.full(1, np.nan, dtype=dtype)
			self.shm = smm.SharedMemory(size=tmp.nbytes)
			self.data = np.ndarray(tmp.shape, dtype=dtype, buffer=self.shm.buf)
			self.data[:] = tmp[:]
		else:
			''' To be used by client processes '''
			self.shm = smm.SharedMemory(name=shm_name)
			self.data = np.ndarray((1,), dtype=dtype, buffer=self.shm.buf)

	# def __setitem__(self):
	# 	pass # TODO update version

	def serialize(self) -> SerializedStruct:
		''' Send to another process ''' 
		return (wire_pickle(self.dtype), self.shm.name)

	def to_pure(self) -> Struct:
		s = Struct(self.dtype)
		s.set(self)
		return s

	def copy(self, smm: AFSharedMemoryManager) -> 'SharedStruct':
		''' Create a non-linked copy of self ''' 
		return SharedStruct.from_pure(smm, self.to_pure())

	@staticmethod
	def deserialize(smm: AFSharedMemoryManager, arg: SerializedStruct) -> 'SharedStruct':
		''' Receive from another process ''' 
		return SharedStruct(smm, wire_unpickle(arg[0]), shm_name=arg[1])

	@staticmethod
	def from_pure(smm: AFSharedMemoryManager, struct: Struct) -> 'SharedStruct':
		''' Construct a shareable Struct from a pure one ''' 
		s = SharedStruct(smm, struct.dtype)
		s.set(struct)
		return s



''' Utility methods ''' 

SharedData = Union[SharedStruct, SharedStreamingArray]

def deserialize_data(smm: AFSharedMemoryManager, ser_data: SerializedData):
	# TODO very brittle
	if len(ser_data) == 2: 
		return SharedStruct.deserialize(smm, ser_data)
	else:
		return SharedStreamingArray.deserialize(smm, ser_data)

def get_ser_dtype(ser_data: SerializedData) -> np.dtype:
	# TODO very brittle
	return wire_unpickle(ser_data[0])

# class SharedData:
# 	pass

# 	@staticmethod
# 	def deserialize(self, arg: Tuple[Any]):
# 		pass
