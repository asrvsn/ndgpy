import numpy as np
from enum import Enum
from typing import Dict, Set
from abc import ABC, abstractmethod

from ndgpy.data import *

''' Common types ''' 

class Resource(Enum):
	zmq_ctx = 1 		# ZMQ context
	mc_url = 2 			# Multicast url
	orch_tx_url = 3 	# Telemetry tx 
	orch_rx_url = 4 	# Telemetry rx
	orch_api = 5 		# Orchestration API
	smm = 6 			# Shared-memory manager
Resources = Dict[Resource, Any]
ResourceSpec = Set[Resource]

''' Interfaces ''' 

class Resourced(ABC):
	''' Resource provision ''' 
	@property 
	def rspec(self) -> ResourceSpec:
		return set()

	async def start(self, res: Resources):
		assert self.rspec == res.keys(), f'Incorrect resources given: {res.keys()}'

	async def stop(self):
		''' Clear the resources, if needed ''' 
		pass

class Parametrized(Resourced):
	''' Parameter provision ''' 
	# TODO: encoding for set-valued parameters
	def __init__(self, smm: AFSharedMemoryManager):
		self.params = SharedStruct.from_pure(smm, self.init_params)
		self.params_serialized = self.params.serialize()

	@property
	def rspec(self):
		return {Resource.smm}

	@property
	@abstractmethod
	def init_params(self) -> Struct:
		# Note: obviously, these can only be numeric/boolean types. Use internal indexing to interpret parameters if using arbitrary discrete types.
		pass

	@property
	def params_lb(self) -> Struct:
		''' Optional lower-bounds for parameters; used for optimization. ''' 
		return None

	@property
	def params_ub(self) -> Struct:
		''' Optional upper-bounds for parameters; used for optimization. ''' 
		return None

	@property 
	def param_bounds(self) -> Dict[str, Tuple[float, float]]:
		lb, ub = self.params_lb.to_dict(), self.params_ub.to_dict()
		for k in lb:
			lb[k] = (lb[k], ub[k])
		return lb

	async def start(self, res: Resources):
		super().start(res)
		self.parameters = deserialize_data(res[Resource.smm], self.params_serialized)