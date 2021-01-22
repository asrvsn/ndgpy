import numpy as np
from .streaming import *

''' Run tests ''' 
arr = StreamingArray(np.int64, lookback=5)
for i in range(10):
	arr.consume(i)
assert (arr[:] == np.array([9, 8, 7, 6, 5])).all()