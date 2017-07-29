import numpy as np

from collections import defaultdict

from pcm.strategy._trade import Trade
from pcm.errors import NoOpenTrade
from pcm.conf import LONG, SHORT, EXIT, BUY, SELL, MKT
from pcm.event import SignalEventPct, OrderEvent
from pcm.errors import OverFilling


class Position:
	"""A security position that assoicates with one Symbol
	- position size, average cost (inc commission), market value
	- position return, holding length
	- check for trade-level hard stop, and rebalance 
	"""
	__slots__ = [
		'symbol', 'pct_portfolio', 'rebalance', 'hard_stop',
		'tick', '_open_trade', 'trades', 'trade_mapper',
		'signals', 'closed_trades',
	]

	signal_lvl = ('hard_stop', 'normal', 'rebalance')


	def __init__(self, symbol, pct_portfolio, rebalance=0, hard_stop=0):
		"""
		Parameter
		---------
		symbol: str, one security ticker
		pct_portfolio: float, portfolio alloation for this position
		rebalance: int, # of days to rebalance, 0 = no rebalance
		hard_stop: float, % of loss on position to hard stop, 0 = no hard stop
		"""
		self.symbol = symbol
		self.pct_portfolio = pct_portfolio
		self.rebalance = rebalance
		self.hard_stop = hard_stop

		self.tick = None
		self._open_trade = None  # there should be only one opening trade
		self.trades = {}  # {trade id: trade instance}
		self.trade_mapper = {}  # {order id: trade id}
		# self.closed_trades = []
		self.signals = defaultdict(lambda: None)


	def __repr__(self):
		return 'Position: t={}, pos={}, quantity={}'.format(
			self.t, self.position.value, self.total_quantity
		)

	@property
	def open_trade(self):
		try:
			return self.trades[self._open_trade]
		except KeyError:
			raise NoOpenTrade('There is any open trade yet')

	@property
	def position(self):
		Q = self.quantity
		if Q:
			if Q > 0:
				return LONG
			else:
				return SHORT
		else:
			return EXIT

	@property
	def has_position(self):
		if len(self.trades):
			return True
		else:
			return False

	@property
	def has_open_orders(self):
		if self.open_quantity:
			return True
		else:
			return False

	@property
	def has_long(self):
		return self.position is LONG

	@property
	def has_short(self):
		return self.position is SHORT

	@property
	def t(self):
		try:
			return self.open_trade.t
		except NoOpenTrade:
			return 0

	@property
	def r(self):
		"""Position return based on average cost including commission"""
		try:
			return self.profit / self.max_cost
		except ZeroDivisionError:
			return -np.inf

	@property
	def cost_basis(self):
		try:
			return self.cost / self.quantity
		except ZeroDivisionError:
			return 0

	@property
	def drawdown(self):
		try:
			return self.max_profit / self.profit - 1
		except ZeroDivisionError:
			return 0

	@property
	def mv(self):
		"""Current Market Value of the position"""
		return sum(t.mv for t in self.trades.values())

	@property
	def total_quantity(self):
		try:
			return self.open_trade.position * self.open_trade.total_quantity
		except NoOpenTrade:
			return 0

	@property
	def open_quantity(self):
		return sum(abs(t.open_quantity) for t in self.trades.values())

	@property
	def quantity(self):
		return sum(t.position * t.quantity for t in self.trades.values())

	@property
	def cost(self):
		return sum(t.cost for t in self.trades.values())

	@property
	def max_cost(self):
		return sum(t.max_cost for t in self.trades.values())

	@property
	def profit(self):
		return sum(t.profit for t in self.trades.values())

	@property
	def max_profit(self):
		return sum(t.max_profit for t in self.trades.values())


	def _reset_signals(self):
		"""A dictionary that stores all generated signal in this heartbeat
		- that is tag for urgency
		- one strategy will can only generate one signal per symbol
		"""
		self.signals.clear()


	def _update_data(self, tick):
		"""Before doing anything
		we need to update the data on given newest market tick

		Parameter:
		----------
		tick (Tick): The tick object with OHLC price and others

		Return:
		-------
		mv_delta: The change in market value between two market tick
		"""
		self.tick = tick

		for trade in self.trades.values():
			trade.on_market(tick)


	def _generate_signal(self, signal_type, lvl, **kws):
		"""Generate a signal that will stored at Strategy level
		- Then all signals will be batch processed

		Parameter
		---------
		signal_type: {LONG, SHORT, EXIT}
		lvl: {'hard_stop', 'normal', 'rebalance'}
			the level of urgency for this signal
		kws: additional arguments passes to the SignalEvent class
			- especially the strength for percentage of portfolio
			- if not passed, will the default
		"""
		if signal_type is EXIT:
			strength = 0
		else:
			strength = kws.get('strength', self.pct_portfolio)

		self.signals[lvl] = SignalEventPct(
			self.symbol, signal_type, strength=strength
		)


	def _calculate_signals(self):
		"""On inbound market event, calculate nesscary signals"""
		if self.has_position:
			self.check_hard_stop()
			self.check_rebalance()


	def check_hard_stop(self):
		"""Max Position Drawdown Hard Stop Indicator"""
		if self.hard_stop == 0: return

		if self.drawdown >= self.hard_stop:
			self._generate_signal(EXIT, lvl='hard_stop')


	def check_rebalance(self):
		"""Rebalancing Indicator on fix time period"""
		# if the strategy have no rebalance period
		if self.rebalance == 0: return

		if self.t % self.rebalance == 0:
			self._generate_signal(self.position, lvl='rebalance')


	def generate_orders(self, equity):
		"""Generate apporiate Order Event on existing signals
		- First it will get the signal based on urgency
		- The calculate target quanity of the order based on existing quantity
		- Figure out the direction of the trade
		- Generate the orders
		- Reset all signals

		Return:
		-------
		yield -> order, lvl
		"""
		signal = None
		signal_lvl = None
		trade_qty = []

		# get signal based on urgency
		for lvl in self.signal_lvl:
			if lvl in self.signals:
				signal = self.signals[lvl]
				signal_lvl = lvl
				break   # only generate one signal per symbol

		# if there is no signal, we return None
		if signal:
			# get the target quantity
			target = signal.target_qty(self.tick.close, equity)

			Q = self.total_quantity
			if Q == 0:  # there is no trade
				trade_qty.append(target)
			elif np.sign(target) == np.sign(Q):  # same direction trade
				trade_qty.append(target - Q)
			else:  # revseing trade
				trade_qty.append(-Q)
				trade_qty.append(target)

		# if we need to trade some quantity
		for q in trade_qty:
			if not q: continue  # if 0 quantity

			if np.sign(q) == 1:
				fill_type = BUY
			elif np.sign(q) == -1:
				fill_type = SELL

			# get the order
			order = OrderEvent(self.symbol, MKT, abs(q), fill_type)
			yield order, signal_lvl

		self._reset_signals()


	def confirm_order(self, order):
		oid = order.id

		try:  # if there is open_trade, need to check to close it or not
			trade = self.open_trade
			trade.on_order(oid, order.quantity, order.direction.value)

			if trade.is_closing:
				self._open_trade = None

		except NoOpenTrade:  # when there is no trade yet, then we open new
			trade = Trade(
				oid, order.quantity,
				order.direction.value, self.tick
			)

			self._open_trade = trade.id
			self.trades[trade.id] = trade
		
		self.trade_mapper[oid] = trade.id


	def on_fill(self, fill):
		"""Update the position on inbound fill event"""
		oid = fill.order_id
		trade_id = self.trade_mapper[oid]

		trade = self.trades[trade_id]
		trade.on_fill(
			oid, fill.quantity, fill.fill_type.value,
			fill.fill_cost, fill.commission
		)

		if trade.is_closed:
			self.trades.pop(trade.id)
			# self.closed_trades.append(trade)
