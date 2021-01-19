import copy
import shortuuid

from ndgpy.data import *
from .base import *
from .interfaces import *

def copy_node(n: Node, smm: AFSharedMemoryManager) -> Node:
	''' Copy a node with new data (parameters, etc.) Does not retain connections. ''' 
	m = copy.copy(n)
	m.id = shortuuid.uuid()
	if isinstance(n, Emitter):
		m.sinks = dict()
	if isinstance(n, Collector):
		m.sources = dict()
	if isinstance(n, Parametrized):
		m.parameters = n.parameters.copy(smm)
		m.parameters_serialized = m.parameters.serialize()
	return m