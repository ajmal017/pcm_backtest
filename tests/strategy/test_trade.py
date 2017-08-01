import numpy as np

from nose.tools import raises

from pcm_backtest.trade import Trade
from pcm_backtest.event import OrderEvent, Tick
from pcm_backtest.conf import MKT, BUY, SELL, SMART, LONG, SHORT, EXIT
from pcm_backtest.errors import OverFilling

from datetime import datetime
from bson import ObjectId



class TestLongTrade:
	def setup(self):
		self.open_tick = Tick(datetime(2011,1,1), 10, 10, 10, 10, 1000)
		self.ticks = [
			Tick(datetime(2011,1,2), 11, 11, 11, 11, 1000),
			Tick(datetime(2011,1,2), 12, 12, 12, 12, 1000),
		]
		self.open_order = OrderEvent('1', MKT, 30, BUY)
		self.sell_order = OrderEvent('1', MKT, 30, SELL)

		self.trade = Trade(
			self.open_order.id, self.open_order.quantity,
			self.open_order.direction.value,
			self.open_tick
		)


	def test_init(self):
		assert self.trade.t == 1
		assert isinstance(self.trade.id, ObjectId)
		assert self.trade.has_open_orders == True
		assert len(self.trade.orders) == 1
		assert len(self.trade.share_queue) == 0
		assert self.trade.position == 1
		assert self.trade.total_quantity == 30
		assert self.trade.open_quantity == 30
		assert self.trade.quantity == 0
		assert self.trade.max_profit == 0
		assert self.trade.max_cost == 0
		assert self.trade.tick is self.open_tick
		assert self.trade.mv == 0
		assert self.trade.cost == 0
		assert self.trade.cost_basis == 0
		assert self.trade.unrealized == 0
		assert self.trade.realized == 0
		assert self.trade.is_closed == False


	def test_as_dict(self):
		assert isinstance(self.trade.as_dict(), dict)


	@raises(OverFilling)
	def test_fill_same_direction_error(self):
		self.trade.on_fill(self.sell_order.id, 10, 1, 10.03, 1)


	@raises(OverFilling)
	def test_fill_opposite_direction_error(self):
		self.trade.on_fill(self.sell_order.id, 10, -1, 10.03, 1)


	def test_fill_full_run(self):
		oid = self.open_order.id
		self.trade.on_fill(oid, 10, 1, 10.03, 1)

		assert oid in self.trade.orders
		assert self.trade.orders[oid]['Q'] == 20
		assert self.trade.open_quantity == 20
		assert self.trade.quantity == 10
		assert np.isclose(self.trade.cost, 101.3)
		assert np.isclose(self.trade.max_cost, 101.3)
		assert np.isclose(self.trade.realized, 0)
		assert np.isclose(self.trade.unrealized, -1.3)
		assert np.isclose(self.trade.r, (-1.3+0) / 101.3)
		assert len(self.trade.share_queue) == 1
		assert self.trade.is_closed == False

		self.trade.on_fill(oid, 10, 1,  10.05, 1)

		assert oid in self.trade.orders
		assert self.trade.orders[oid]['Q'] == 10
		assert self.trade.open_quantity == 10
		assert self.trade.quantity == 20
		assert np.isclose(self.trade.cost, 202.8)
		assert np.isclose(self.trade.max_cost, 202.8)
		assert np.isclose(self.trade.realized, 0)
		assert np.isclose(self.trade.unrealized, -2.8)
		assert np.isclose(self.trade.r, (-2.8+0) / 202.8)
		assert len(self.trade.share_queue) == 2
		assert self.trade.is_closed == False

		self.trade.on_fill(oid, 10, 1,  10.10, 1)

		assert oid not in self.trade.orders
		assert self.trade.open_quantity == 0
		assert self.trade.quantity == 30
		assert np.isclose(self.trade.cost, 304.8)
		assert np.isclose(self.trade.max_cost, 304.8)
		assert np.isclose(self.trade.realized, 0)
		assert np.isclose(self.trade.unrealized, -4.8)
		assert np.isclose(self.trade.r, (-4.8+0) / 304.8)
		assert len(self.trade.share_queue) == 3
		assert self.trade.is_closed == False

		self.trade.on_market(self.ticks[0])
		assert self.trade.t == 2

		oid = self.sell_order.id
		self.trade.on_order(
			self.sell_order.id, self.sell_order.quantity,
			self.sell_order.direction.value
		)
		assert self.trade.is_closing == True

		self.trade.on_fill(oid, 5, -1,  10.59, 1)
		assert oid in self.trade.orders
		assert self.trade.orders[oid]['Q'] == 25
		assert self.trade.open_quantity == -25
		assert self.trade.quantity == 25
		assert np.isclose(self.trade.cost, 254.15)
		assert np.isclose(self.trade.max_cost, 304.8)
		assert np.isclose(self.trade.realized, 1.3)
		assert np.isclose(self.trade.unrealized, 20.85)
		assert np.isclose(self.trade.r, (20.85+1.3) / 304.8)
		assert len(self.trade.share_queue) == 3
		assert self.trade.is_closed == False

		self.trade.on_fill(oid, 15, -1,  10.58, 1)
		assert oid in self.trade.orders
		assert self.trade.orders[oid]['Q'] == 10
		assert self.trade.open_quantity == -10
		assert self.trade.quantity == 10
		assert np.isclose(self.trade.cost, 102)
		assert np.isclose(self.trade.max_cost, 304.8)
		assert np.isclose(self.trade.realized, 6.85)
		assert np.isclose(self.trade.unrealized, 8)
		assert np.isclose(self.trade.r, (8+6.85) / 304.8)
		assert len(self.trade.share_queue) == 1
		assert self.trade.is_closed == False

		self.trade.on_fill(oid, 10, -1,  10.58, 1)
		assert oid not in self.trade.orders
		assert self.trade.open_quantity == 0
		assert self.trade.quantity == 0
		assert len(self.trade.share_queue) == 0
		assert self.trade.is_closed == True



class TestShortTrade:
	def setup(self):
		self.open_tick = Tick(datetime(2011,1,1), 10, 10, 10, 10, 1000)
		self.ticks = [
			Tick(datetime(2011,1,2), 11, 11, 11, 11, 1000),
			Tick(datetime(2011,1,2), 12, 12, 12, 12, 1000),
		]
		self.open_order = OrderEvent('1', MKT, 30, SELL)
		self.sell_order = OrderEvent('1', MKT, 30, BUY)

		self.trade = Trade(
			self.open_order.id, self.open_order.quantity,
			self.open_order.direction.value,
			self.open_tick
		)


	def test_status(self):
		assert self.trade.is_closed == False

		self.trade.on_order(
			self.sell_order.id, self.sell_order.quantity,
			self.sell_order.direction.value,
		)
		assert self.trade.is_closing
		assert not self.trade.is_closed


	def test_init(self):
		assert self.trade.t == 1
		assert isinstance(self.trade.id, ObjectId)
		assert self.trade.has_open_orders == True
		assert len(self.trade.orders) == 1
		assert len(self.trade.share_queue) == 0
		assert self.trade.position == -1
		assert self.trade.total_quantity == 30
		assert self.trade.open_quantity == 30
		assert self.trade.quantity == 0
		assert self.trade.max_profit == 0
		assert self.trade.max_cost == 0
		assert self.trade.tick is self.open_tick
		assert self.trade.mv == 0
		assert self.trade.cost == 0
		assert self.trade.cost_basis == 0
		assert self.trade.unrealized == 0
		assert self.trade.realized == 0
		assert self.trade.is_closed == False


	def test_as_dict(self):
		assert isinstance(self.trade.as_dict(), dict)


	@raises(OverFilling)
	def test_fill_same_direction_error(self):
		self.trade.on_fill(self.sell_order.id, 10, 1, 10.03, 1)


	@raises(OverFilling)
	def test_fill_opposite_direction_error(self):
		self.trade.on_fill(self.sell_order.id, 10, -1, 10.03, 1)


	def test_fill_full_run(self):
		oid = self.open_order.id
		self.trade.on_fill(oid, 10, -1, 9.97, 1)

		assert oid in self.trade.orders
		assert self.trade.orders[oid]['Q'] == 20
		assert self.trade.open_quantity == 20
		assert self.trade.quantity == 10
		assert np.isclose(self.trade.cost, 98.7)
		assert np.isclose(self.trade.max_cost, 98.7)
		assert np.isclose(self.trade.realized, 0)
		assert np.isclose(self.trade.unrealized, -1.3)
		assert np.isclose(self.trade.r, (-1.3+0) / 98.7)
		assert len(self.trade.share_queue) == 1
		assert self.trade.is_closed == False

		self.trade.on_fill(oid, 10, -1,  9.95, 1)

		assert oid in self.trade.orders
		assert self.trade.orders[oid]['Q'] == 10
		assert self.trade.open_quantity == 10
		assert self.trade.quantity == 20
		assert np.isclose(self.trade.cost, 197.2)
		assert np.isclose(self.trade.max_cost, 197.2)
		assert np.isclose(self.trade.realized, 0)
		assert np.isclose(self.trade.unrealized, -2.8)
		assert np.isclose(self.trade.r, (-2.8+0) / 197.2)
		assert len(self.trade.share_queue) == 2
		assert self.trade.is_closed == False

		self.trade.on_fill(oid, 10, -1,  9.9, 1)

		assert oid not in self.trade.orders
		assert self.trade.open_quantity == 0
		assert self.trade.quantity == 30
		assert np.isclose(self.trade.cost, 295.2)
		assert np.isclose(self.trade.max_cost, 295.2)
		assert np.isclose(self.trade.realized, 0)
		assert np.isclose(self.trade.unrealized, -4.8)
		assert np.isclose(self.trade.r, (-4.8+0) / 295.2)
		assert len(self.trade.share_queue) == 3
		assert self.trade.is_closed == False

		self.trade.on_market(self.ticks[0])
		assert self.trade.t == 2

		oid = self.sell_order.id
		self.trade.on_order(
			self.sell_order.id, self.sell_order.quantity,
			self.sell_order.direction.value
		)
		assert self.trade.is_closing == True

		self.trade.on_fill(oid, 5, 1,  10.59, 1)
		assert oid in self.trade.orders
		assert self.trade.orders[oid]['Q'] == 25
		assert self.trade.open_quantity == -25
		assert self.trade.quantity == 25
		assert np.isclose(self.trade.cost, 245.85)
		assert np.isclose(self.trade.max_cost, 295.2)
		assert np.isclose(self.trade.realized, -4.6)
		assert np.isclose(self.trade.unrealized, -29.15)
		assert np.isclose(self.trade.r, (-29.15-4.6) / 295.2)
		assert len(self.trade.share_queue) == 3
		assert self.trade.is_closed == False

		self.trade.on_fill(oid, 15, 1,  10.58, 1)
		assert oid in self.trade.orders
		assert self.trade.orders[oid]['Q'] == 10
		assert self.trade.open_quantity == -10
		assert self.trade.quantity == 10
		assert np.isclose(self.trade.cost, 98)
		assert np.isclose(self.trade.max_cost, 295.2)
		assert np.isclose(self.trade.realized, -16.45)
		print(self.trade.unrealized)
		assert np.isclose(self.trade.unrealized, -12)
		assert np.isclose(self.trade.r, (-16.45-12) / 295.2)
		assert len(self.trade.share_queue) == 1
		assert self.trade.is_closed == False

		self.trade.on_fill(oid, 10, 1,  10.58, 1)
		assert oid not in self.trade.orders
		assert self.trade.open_quantity == 0
		assert self.trade.quantity == 0
		assert len(self.trade.share_queue) == 0
		assert self.trade.is_closed == True
