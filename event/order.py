import pandas as pd

from pandas import Timestamp
from bson import ObjectId

from pcm_backtest.conf import ORDER, ORDER_DICT, FILL_DICT
from .core import Event



class OrderEvent(Event):
	"""
	Handles the event of sending a order to an execution system
	The order contains a symbol (e.g. AAPL), a type (market or limit),
	quantity and a direction
	"""
	__slots__ = ['id', 'symbol', 'order_type', 'quantity', 'direction']
	type = ORDER

	def __init__(self, symbol, order_type, quantity, direction, id=None):
		"""
		Initialize the order type, setting whether it is
		a Market Order ('MKT') or Limit Order ('LMT'),
		has a quantity (integer)
		and its direction ('BUY' or 'SELL')

		Parameter
		---------
		id (ObjectId): unique identifier of the order
		symbol: The instructment to trade
		timestamp: The timestamp at which the order is placed
		order_type: 'MKT' or 'LMT'
		quantity: Non-negative Integer for quantity
		direction: 'BUY' or 'SELL' for long or short
		"""
		self.id = ObjectId() if id is None else id
		self.symbol = symbol
		self.order_type = order_type
		self.quantity = quantity
		self.direction = direction


	def __repr__(self):
		return (
			(
				"Order: id=%s, Timestamp=%s, Symbol=%s, "
				+ "Type=%s, Quantity=%s, Direction=%s"
			) % (
				self.id, self.timestamp, self.symbol, self.order_type.value,
				self.quantity, self.direction.__name__
			)
		)


	def __lt__(self, other):
		return not self.id < other.id


	def __eq__(self, other):
		return self.id == other.id


	@property
	def timestamp(self):
		return pd.Timestamp(self.id.generation_time)


	def as_dict(self):
		return {
			'event_type': 'order',
			'data': {
				'id': str(self.id),
				'symbol': self.symbol,
				'order_type': self.order_type.value[0].upper(),
				'quantity': self.quantity,
				'direction': self.direction.value,
			},
		}
	
	
	@classmethod
	def from_dict(cls, **kws):
		return cls(
			id=ObjectId(kws.get('id')),
			symbol=kws.get('symbol'),
			order_type=ORDER_DICT[kws.get('order_type')],
			quantity=kws.get('quantity'),
			direction=FILL_DICT[kws.get('direction')],
		)
