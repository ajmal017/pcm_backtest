import logging, time, numpy as np

from bson import ObjectId
from math import floor
from collections import defaultdict, OrderedDict, deque
from blinker import ANY

from .base import BaseConsumer
from .conf import SMART
from .event import FillEventIB

logger = logging.getLogger('Executor')



class Executor(BaseConsumer):
	"""Simulated Order Executor for Backtesting

	Features:
	---------
	- Fully Decoupled
	- Register multiple Strategies at different time
	- Build in Order Book that track fillings at Strategy Level
	- Simulated Slippage Model and Price Impact Estimation
	"""
	def __init__(self):
		self.books = {}
		super().__init__(comp_type='EXE', required=['feed'])


	def subscriptions(self):
		return [
			('reg-exe', ANY, self.on_reg),
			('dereg-exe', ANY, self.on_dereg),
			('ack-reg-feed', self.id, self.on_ack_reg_feed),
			('order', ANY, self.on_order),
			('tick', ANY, self.on_market),
		]


	def on_reg(self, oid, body):
		"""Handler for `reg-exe` event

		- We simply create a separeate Order book
			for this newly regsiter strategy
		"""
		if body['group'] == 'stgy':
			if oid not in self.books:
				book = SimuBook(self, oid)
				self.books[oid] = book
				# self.basic_publish('next', sender=book.stgy_oid)

		self.basic_publish('ack-reg-exe', sender=oid)


	def on_dereg(self, oid, body):
		"""Handler for `dereg-exe` event

		- Remove the Order book for this strategy
		"""
		if body['group'] == 'stgy':
			try:
				self.books.pop(oid)
			except KeyError:
				pass

		self.basic_publish('ack-dereg-exe', sender=oid)


	def on_ack_reg_feed(self, oid, body):
		"""Handler for `ack-reg-feed` event

		- For simulated order executor, we need data from Feeder
		- Thus, we need a way to make sure the feeder is running and acked us
		"""
		self.required['feed'] = True


	def on_market(self, stgy_oid, body):
		self.books[stgy_oid].on_market(body['ticks'])


	def on_order(self, stgy_oid, body):
		self.books[stgy_oid].on_order(body['order'])



class SimuBook:
	"""Simulated Order Book for one specific Strategy

	- In backtesting environment, we would like to separeate
		the impact from different strategies to isolate the evaluation
	- 
	"""
	__slots__ = [
		'app', 'stgy_oid', 'orders',
		'fillings', 'ticks', 'filled_counter',
	]
	
	def __init__(self, app, stgy_oid):
		self.app = app
		self.stgy_oid = stgy_oid
		self.orders = deque()  # need to append back to the head
		self.fillings = OrderedDict()  # keep the order orders
		self.ticks = None

		# counter to make sure multiple orders same symbol siutation
		self.filled_counter = defaultdict(int)


	def on_order(self, order):
		"""Handler for `order.stgy_name` event

		- Add new arrivals to filling books
		- Then put them to queue waiting to be executed
		"""
		if order.id not in self.fillings:
			self.fillings[order.id] = {
				'order': order, 'fills': [],
				'open_quantity': order.quantity,
				'status': 'SUBMITTED'
			}
		
		logger.info('Got Order={} from Strategy={}'.format(order.id, self.stgy_oid))
		self.orders.append(order.id)


	def on_market(self, ticks):
		"""Handler for `tick.stgy_name` event
		
		- Try to fill as much as we can
		- It not filled, we postpone them to next bars
		- Then request next bar data to fill more
		"""
		self.ticks = ticks

		# filling order when new tickk arrives
		while True:
			try:
				oid = self.orders.popleft()
				order = self.fillings[oid]['order']

				self.fillings[oid]['status'] = 'FILLING'
				self.place_order(order)
			except IndexError:
				break

		# requeue back not filled orders and record filled ones
		# We don't do it along with filling
		# to postpone the not fully filled orders to next bars
		for oid in list(self.fillings.keys()):
			if self.fillings[oid]['status'] != 'FILLED':
				self.orders.append(oid)
			else:
				transact = self.fillings.pop(oid)
			
		# reset counter
		self.filled_counter.clear()
		
		# request new bar data
		# self.app.basic_publish('next', sender=self.stgy_oid)


	def place_order(self, order):
		"""Place the Order, simple

		- First we need to get the outstanding quantity for this order
		- Estimate the filled quantity and price impacts
		- Update the filling book
		- Then we publish Fill event & Simu-Impact Event
		"""
		symbol, oid = order.symbol, order.id
		tick = self.ticks[symbol]

		open_q = self.fillings[oid]['open_quantity']

		# get filled and simulated volume share impacts
		filled, impacted_price = slippage(
			# use mid-price for mimic wap price for fillings
			(tick.high+tick.low+tick.close)/3,
			tick.volume, open_q, order.direction.value,
			filled_volume=max(0, self.filled_counter[symbol]),
		)
		self.filled_counter[symbol] += filled
		
		if filled:
			# update filling books
			open_q -= filled
			if open_q == 0:
				self.fillings[oid]['status'] = 'FILLED'
			
			self.fillings[oid]['open_quantity'] = open_q

			# create fill events:
			fill_event = FillEventIB(
				order_id=oid, symbol=symbol, exchange=SMART, quantity=filled,
				fill_type=order.direction, fill_cost=impacted_price
			)
			self.fillings[oid]['fills'].append(fill_event)

			# publish fill events
			logger.info(
				'Publish Fill for Q={} Open={} for Order={}'
				.format(fill_event.quantity, open_q, oid)
			)
			self.app.basic_publish('fill', sender=self.stgy_oid, fill=fill_event)



SLIPPAGE_LIMIT = 0.025  # default 2.5% max volume tradable bar
MIN_IMPACT = 0.003  # minimal bid-ask spread loss per share
PRICE_IMPACT_COEF = 0.1


def slippage(
	price, bar_volume, open_quantity, direction,
	filled_volume=0, slippage_limit=SLIPPAGE_LIMIT,
	min_impact=MIN_IMPACT, impact_coef=PRICE_IMPACT_COEF
):
	"""Slippage Estimator using Volume Share Model

	Theory:
	-------
	The fillable quantity is a fix portion of the bar's total traded volume
		thus V is the percentage of total volume that can be traded

	Give V and actual traded quantity portion V'
		simulated price impact I is estimated as
		using piece-wise linear function:
		I = max(Min Impact, V'**2 * Impact Coef * Price)


	Parameter:
	----------
	price (float): The current bar WAP price, using VWAP Best Effor method
	volume (bigint): The current bar's trading volume
	open_quantity (bigint): Quantity remaining on the Order
	direction {1, -1}: Whether the order is buying or selling
	slippage_limit (float): The max tradable volume percentage
	min_impact (float): The mininum price impact per share
	impact_coef (float): The coef used in linear simuated price impact

	Return:
	-------
	Hypothestical Filled Quanitty
	Simulated Price Impact in term of geometric percentage
	"""
	if bar_volume <= 0:
		return 0, 0

	filled = floor(min(
		open_quantity,
		max(0, slippage_limit * bar_volume - filled_volume)
	))

	share = min(filled/bar_volume, slippage_limit)
	impact = direction * max(min_impact, share ** 2 * impact_coef * price)
	return filled, round(impact + price, 3)
