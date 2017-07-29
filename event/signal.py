import pandas as pd

from math import floor
from pandas import Timestamp
from bson import ObjectId

from pcm.conf import SIGNAL, SIGNAL_DICT
from .core import Event



class SignalEventFixed(Event):
	"""
	Expected Strength = # times of 100 shares
	"""
	__slots__ = ['symbol','signal_type','strength','id']
	type = SIGNAL
	event_type = 'signal_fixed'


	def __init__(self, symbol, signal_type, strength=1, id=None):
		"""
		Parameter
		---------
		symbol: The ticker symbol, e.g. 'AAPL'
		signal_type: 'LONG', 'SHORT', 'EXIT'
		strength: float, default=1
			bound with in 0 to positive infinity
		"""
		self.id = ObjectId() if id is None else id
		self.symbol = symbol
		self.signal_type = signal_type
		self.strength = strength


	def __repr__(self):
		return "Strategy: Symbol={}, Type={}, Strength={}".format(
			self.symbol, self.signal_type.value, self.strength
		)


	def __lt__(self, other):
		# for case need to sort signal in a list
		# they will be sorted by the time they were generated
		return not self.id < other.id


	def __eq__(self, other):
		return self.id == other.id


	@property
	def timestamp(self):
		return pd.Timestamp(self.id.generation_time)


	def target_qty(self, price, equity):
		return self.signal_type.sign * floor(self.strength / price * 100)


	def as_dict(self):
		return {
			'event_type': self.event_type,
			'data': {
				'id': str(self.id),
				'symbol': self.symbol,
				'signal_type': self.signal_type.value[0].upper(),
				'strength': self.strength
			},
		}
	
	
	@classmethod
	def from_dict(cls, **kws):
		return cls(
			id=ObjectId(kws.get('id')),
			symbol=kws.get('symbol'),
			signal_type=SIGNAL_DICT[kws.get('signal_type')],
			strength=kws.get('strength'),
		)


class SignalEventPct(SignalEventFixed):
	"""
	Expected Strength = % of Portfolio / Market Price 
	"""
	event_type = 'signal_pct'

	def target_qty(self, price, equity):
		return self.signal_type.sign * floor(self.strength / price * equity)
