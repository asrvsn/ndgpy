''' Data structures ''' 

from typing import Union
import numpy as np

from .base import Struct

class StreamingDataFrame:
	''' Streaming pd.DataFrame-like API with history. For use with numerical data types only. 
	TODO: fix this to use StreamingArray and allow per-column dtypes
	''' 
	def __init__(self, columns: list, dtype: np.dtype, buf_size = 100):
		self.buf_size = buf_size 
		self.size = buf_size*2
		self.head_index = 0
		self.columns = dict(zip(columns, range(len(columns))))
		self.data = np.empty((len(columns), self.size), dtype=dtype)
		self.data[:] = np.nan

	def set(self, k: str, i: int, v: float):
		if self.head_index >= i:
			self.data[self.columns[k], self.head_index-i] = v

	def get(self, k: str, i: int, default=0):
		if self.head_index >= i:
			return self.data[self.columns[k], self.head_index-i]
		else:
			return default

	def get_re(self, k: str, i: int, default=0):
		return np.real(self.get(k, i, default))

	def get_im(self, k: str, i: int, default=0):
		return np.imag(self.get(k, i, default))

	def get_col(self, k: str):
		return self.data[self.columns[k], :self.head_index+1]

	def advance(self):
		self.head_index += 1
		if self.head_index == self.size:
			self.data[:, 0:self.buf_size] = self.data[:, self.buf_size:]
			self.data[:, self.buf_size:] = np.nan
			self.head_index = self.buf_size

	@property
	def length(self):
		return self.head_index


class StreamingArray:
	''' Streaming np.array-like API with history. 1-dimensional representation. For use with numerical data types only. ''' 
	def __init__(self, dtype: np.dtype, buf_size: int):
		self.buf_size = buf_size 
		self.size = buf_size*2
		self.head_index = self.size-1
		self.data = np.full(self.size, np.nan, dtype=dtype)

	def consume(self, v: Union[Struct, np.void]):
		''' Consume new data and advance head pointer ''' 
		self.head_index -= 1
		if self.head_index == -1:
			self.data[self.buf_size:] = self.data[:self.buf_size]
			self.data[:self.buf_size] = np.nan
			self.head_index = self.buf_size - 1
		if type(v) == np.void:
			self.data[self.head_index] = v 
		else:
			self.data[self.head_index] = v.numpy()

	def __getitem__(self, slice):
		if type(slice) == int:
			assert slice >= 0, 'Cannot use negative index in StreamingArray'
			assert slice <= self.length-1, 'Slice out of bounds'
			return self.data[self.head_index+slice]
		else:
			start, stop = slice.start, slice.stop 
			if start is None: start = 0
			if stop is None: stop = self.head_index
			assert start >= 0 and stop >= 0 and stop >= start, 'Cannot use negative slice values in StreamingArray'
			assert start <= self.length-1 and stop <= self.length-1, 'Slice out of bounds'
			return self.data[self.head_index+start:self.head_index+stop+1:slice.step]

	def __len__(self):
		return self.length

	@property
	def length(self):
		return self.size - self.head_index - 1

	@property
	def will_reshuffle(self):
		return self.head_index == 0

	@property
	def dtype(self):
		return self.data.dtype
	
