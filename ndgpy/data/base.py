from typing import Union, List, Any, Dict
import numpy as np
import xxhash

from ndgpy.utils import flatten

class Struct: 
	def __init__(self, dtype: np.dtype, fill=np.nan):
		self.data = np.full(1, fill, dtype=dtype)
		assert self.names is not None, 'Struct requires structured data types'
		self.is_item = len(self.names) == 1

	def __getitem__(self, idx):
		return self.data[0].__getitem__(idx)

	def __setitem__(self, idx, val):
		self.data[0].__setitem__(idx, val)

	def __repr__(self):
		return self.data[0].__repr__()

	@property
	def dtype(self):
		return self.data.dtype

	@property
	def names(self):
		return self.data.dtype.names

	@property 
	def flat(self):
		return flatten(self.data[0])

	def numpy(self) -> np.void:
		return self.data[0]

	def set(self, other: Union['Struct', np.void]):
		assert other.dtype == self.dtype
		if type(other) in (np.void, tuple):
			self.data[0] = other
		else:
			self.data[0] = other.data[0]

	def merge(self, other: Union['Struct', np.void]):
		self[other.dtype.names] = other[:]

	def is_empty(self) -> bool:
		return self.names == ()

	def item(self) -> float:
		v = self.data.item()
		return v[0] if self.is_item else v

	def to_dict(self, delimiter='$') -> Dict[str, Any]:
		# TODO
		pass

	@staticmethod
	def from_dict(self, d: Dict[str, Any], delimeter='$'):
		# TODO 
		pass

	def __hash__(self) -> int:
		h = xxhash.xxh64()
		h.update(self.data.view(np.byte).data)
		h.update(bytes(str(self.data.dtype), 'utf-8')) # Disambiguate by dtype
		return h.intdigest()
