import pandas as pd

from pandas import Timestamp
from abc import abstractmethod
from bson import ObjectId

from pcm.conf import EXCHANGE_DICT, FILL, FILL_DICT
from .core import Event



class FillEvent(Event):
	"""
	Encapuslate the notion of a Filled Order, as returned
	from a brokage. Stores the quantity and filled price of an instructment.
	In addition, stores the commission of the trade from the brokage
	"""
	__slots__ = [
		'symbol', 'exchange', 'quantity',
		'fill_type', 'fill_cost', 'commission', 'id', 'order_id'
	]
	type = FILL

	def __init__(
		self, order_id,
		symbol, exchange, quantity, fill_type, fill_cost,
		commission=None, id=None
	):
		"""
		Initialize the FillEvent object.

		Parameter
		---------
		symbol: The instrucment which was filled
		timestamp: The bar-resoultion when the order was filled
		exchange: The exchange where the order was filled
		quantity: The filled quantity
		fill_type: The direction of fill ('BUY' or 'SELL')
		fill_cost: The holdings value per unit of security
		commission: An optional commission
		"""
		self.order_id = order_id
		self.id = ObjectId() if id is None else id
		self.symbol = symbol
		self.exchange = exchange
		self.quantity = int(quantity)
		self.fill_type = fill_type
		self.fill_cost = fill_cost

		# Calculate commission
		if commission:
			self.commission = commission
		else:
			self.commission = self.calculate_commission()


	def __lt__(self, other):
		return not self.id < other.id


	def __eq__(self, other):
		return self.id == other.id


	def __repr__(self):
		return (
			'Fill: timestamp=%s, Symbol=%s, Exchange=%s, ' +
			'Quantity=%s, Fill Type=%s, ' +
			'Fill Cost=%s, Commission=%s' 
		) % ( 
			self.timestamp, self.symbol, self.exchange.value,
			self.quantity, self.fill_type.__name__,
			self.fill_cost, self.commission
		)


	@property
	def timestamp(self):
		return pd.Timestamp(self.id.generation_time)


	@abstractmethod
	def calculate_commission(self):
		raise NotImplementedError('Commission bases on different broker')



class FillEventIB(FillEvent):
	def calculate_commission(self):
		"""
		Calculate the fees of trading based on Interactive Brokers
		Broker fee structure for API, in USD

		Notes
		-----
		- This does not include exchange or ECN fees

		Reference
		---------
		Based on "US API Directed Orders":
		https://www.interactivebrokers.com/en/index.php?f=commission&p=stocks2
		"""
		min_cost = 1  # have it here, but I decide not to implement to on scale
		max_cost = 0.005*self.fill_cost*self.quantity
		full_cost = 0.005 * self.quantity
		
		return min(full_cost, max_cost)


	def as_dict(self):
		return {
			'event_type': 'fill_ib',
			'data': {
				'id': str(self.id),
				'order_id': str(self.order_id),
				'symbol': self.symbol,
				'exchange': self.exchange.value[0].upper(),
				'quantity': self.quantity,
				'fill_type': self.fill_type.value,
				'fill_cost': self.fill_cost,
				'commission': self.commission,
			}
		}
	
	
	@classmethod
	def from_dict(cls, **kws):
		return cls(
			id=ObjectId(kws.get('id')),
			order_id=ObjectId(kws.get('order_id')),
			symbol=kws.get('symbol'),
			exchange=EXCHANGE_DICT[kws.get('exchange')],
			quantity=int(kws.get('quantity')),
			fill_type=FILL_DICT[kws.get('fill_type')],
			fill_cost=kws.get('fill_cost'),
			commission=kws.get('commission'),
		)
