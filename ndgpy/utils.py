import cloudpickle as pickle
from typing import Any, Iterable, List, Tuple, Callable
import numpy as np
from datetime import datetime

def wire_pickle(x: Any): 
	''' Serialize data for transmission over sockets ''' 
	return pickle.dumps(x).decode('latin1')

def wire_unpickle(x: str):
	return pickle.loads(x.encode('latin1'))

def now():
	return np.datetime64(datetime.now())