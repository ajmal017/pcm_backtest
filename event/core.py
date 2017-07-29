from abc import ABCMeta, abstractmethod
from pandas import json


class Event(metaclass=ABCMeta):
	"""
	Base class for providing an interface for all subsequent
	(inherited) events, that will trigger further events in the 
	trading infrastructure
	"""
	__slots__ = ['type']


	@abstractmethod
	def as_dict(self):
		raise NotImplementedError('Need to overwrite me')


	@abstractmethod
	def from_dict(self, **kws):
		raise NotImplementedError('Need to overwrite me')

	def as_json(self):
		return json.dumps(self.as_dict())
	
	
	@classmethod
	def from_json(cls, string):
		items = json.loads(string)
		return cls.from_dict(**items)
