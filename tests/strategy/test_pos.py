import numpy as np

from nose.tools import raises
from bson import ObjectId
from datetime import datetime
from mock import patch, PropertyMock

from pcm_backtest.pos import Position
from pcm_backtest.trade import Trade
from pcm_backtest.event import OrderEvent, Tick, SignalEventPct, FillEventIB
from pcm_backtest.errors import NoOpenTrade
from pcm_backtest.conf import MKT, BUY, SELL, EXIT, LONG, SHORT, SMART



class TestPosition:
	def setup(self):
		self.pos = Position('A', 0.5, rebalance=5, hard_stop=0.1)
		self.open_tick = Tick(datetime(2011,1,1), 10, 10, 10, 10, 1000)
		self.sell_tick = Tick(datetime(2011,1,2), 10, 10, 10, 10, 1000)


	def test_init(self):
		assert self.pos.tick is None
		assert self.pos._open_trade is None
		assert self.pos.trades == {}
		assert self.pos.trade_mapper == {}
		# assert self.pos.closed_trades == []
		assert isinstance(self.pos.signals, dict)
		assert self.pos.signal_lvl == ('hard_stop', 'normal', 'rebalance')

		assert self.pos.position is EXIT
		assert self.pos.t == 0
		assert self.pos.has_open_orders == False
		assert self.pos.has_long == False
		assert self.pos.has_short == False
		
		assert self.pos.max_profit == 0
		assert self.pos.profit == 0
		assert self.pos.cost == 0
		assert self.pos.max_cost == 0
		assert self.pos.quantity == 0
		assert self.pos.open_quantity == 0
		assert self.pos.total_quantity == 0
		assert self.pos.mv == 0
		assert self.pos.drawdown == 0
		assert self.pos.cost_basis == 0
		assert self.pos.r == -np.inf

		assert 't=0' in str(self.pos)


	@raises(NoOpenTrade)
	def test_no_open_trade(self):
		self.pos.open_trade


	def test_update_data(self):
		self.pos._update_data(self.open_tick)

		assert self.pos.tick is not None
		assert self.pos.tick.volume == 1000


	def test_position_market_has_pos(self):
		self.pos._update_data(self.open_tick)
		self.pos._generate_signal(SHORT, 'normal')

		for order, lvl in self.pos.generate_orders(10000):
			oid = order.id
			self.pos.confirm_order(order)

		self.pos.on_fill(FillEventIB(oid, 'A', SMART, 300, SELL, 10.01))
		self.pos._update_data(self.sell_tick)

		assert self.pos.open_trade.tick is self.sell_tick


	def test_generate_signal(self):
		self.pos._generate_signal(LONG, 'normal')
		signal = self.pos.signals['normal']

		assert isinstance(signal, SignalEventPct)
		assert signal.symbol == self.pos.symbol
		assert signal.strength == 0.5
		assert signal.signal_type is LONG


	def test_new_order_no_pos(self):
		self.pos._update_data(self.open_tick)
		for order, lvl in self.pos.generate_orders(10000):
			assert order is None
			assert lvl is None

		self.pos._generate_signal(LONG, 'normal')
		for order, lvl in self.pos.generate_orders(10000):
			assert order.direction is BUY
			assert order.quantity == 500
			assert isinstance(order, OrderEvent)
			assert lvl == 'normal'

		assert self.pos.signals['normal'] is None

		self.pos._generate_signal(LONG, 'normal', strength=0)
		for order, lvl in self.pos.generate_orders(10000):
			assert order is None
			assert lvl == 'normal'


	@patch('pcm_backtest.pos.Position.check_rebalance')
	@patch('pcm_backtest.pos.Position.check_hard_stop')
	@patch('pcm_backtest.pos.Position.has_position', new_callable=PropertyMock)
	def test_calculate_signal(self, mock_has_position, mock_hs, mock_reb):
		mock_has_position.return_value = False
		self.pos._calculate_signals()
		mock_hs.assert_not_called()
		mock_reb.assert_not_called()

		mock_has_position.return_value = True
		self.pos._calculate_signals()
		mock_hs.assert_called_once()
		mock_reb.assert_called_once()


	@patch('pcm_backtest.pos.Position._generate_signal')
	@patch('pcm_backtest.pos.Position.drawdown', new_callable=PropertyMock)
	def test_calculate_signal(self, mock_drawdown, mock_gen_signal):
		self.pos.hard_stop = 0
		self.pos.check_hard_stop()
		mock_gen_signal.assert_not_called()

		self.pos.hard_stop = 0.1
		mock_drawdown.return_value = 0.09
		self.pos.check_hard_stop()
		mock_gen_signal.assert_not_called()

		mock_drawdown.return_value = 0.11
		self.pos.check_hard_stop()
		mock_gen_signal.assert_called_once()


	@patch('pcm_backtest.pos.Position._generate_signal')
	@patch('pcm_backtest.pos.Position.t', new_callable=PropertyMock)
	def test_calculate_signal(self, mock_t, mock_gen_signal):
		self.pos.rebalance = 0
		self.pos.check_rebalance()
		mock_gen_signal.assert_not_called()

		self.pos.rebalance = 5
		mock_t.return_value = 1
		self.pos.check_rebalance()
		mock_gen_signal.assert_not_called()

		self.pos.rebalance = 5
		mock_t.return_value = 5
		self.pos.check_rebalance()
		mock_gen_signal.assert_called_once()


	def test_position_has_pos_long(self):
		self.pos._update_data(self.open_tick)
		assert not self.pos.has_open_orders

		self.pos._generate_signal(LONG, 'normal')

		for order, lvl in self.pos.generate_orders(10000):
			oid = order.id
			self.pos.confirm_order(order)

		self.pos.on_fill(FillEventIB(oid, 'A', SMART, 300, BUY, 10.01))

		assert self.pos.position is LONG
		assert self.pos.has_open_orders


	def test_position_has_pos_long(self):
		self.pos._update_data(self.open_tick)
		self.pos._generate_signal(SHORT, 'normal')

		for order, lvl in self.pos.generate_orders(10000):
			oid = order.id
			self.pos.confirm_order(order)

		self.pos.on_fill(FillEventIB(oid, 'A', SMART, 300, SELL, 10.01))

		assert self.pos.position is SHORT


	def test_confirm_order(self):
		self.pos._update_data(self.open_tick)
		self.pos._generate_signal(LONG, 'normal')
		for order, lvl in self.pos.generate_orders(10000):
			self.pos.confirm_order(order)

		assert len(self.pos.trades) == 1
		assert self.pos._open_trade in self.pos.trades
		assert self.pos.open_trade.position == 1
		assert self.pos.open_trade.total_quantity == 500
		for k, v in self.pos.trade_mapper.items():
			assert v in self.pos.trades

		self.pos._generate_signal(SHORT, 'normal')
		orders = list(self.pos.generate_orders(10000))
		self.pos.confirm_order(orders[0][0])

		assert len(self.pos.trades) == 1
		assert self.pos._open_trade is None
		assert self.pos.trade_mapper[orders[0][0].id] in self.pos.trades

		self.pos.confirm_order(orders[1][0])

		assert len(self.pos.trades) == 2
		assert self.pos._open_trade in self.pos.trades
		assert self.pos.open_trade.position == -1
		assert self.pos.open_trade.total_quantity == 500

		self.pos._generate_signal(EXIT, 'normal')
		for order, lvl in self.pos.generate_orders(10000):
			print(order)
			self.pos.confirm_order(order)

		assert len(self.pos.trades) == 2
		assert self.pos._open_trade is None


	def test_fills(self):
		self.pos._update_data(self.open_tick)
		self.pos._generate_signal(LONG, 'normal')
		assert not self.pos.has_position

		for order, lvl in self.pos.generate_orders(10000):
			oid = order.id
			self.pos.confirm_order(order)

		assert self.pos.has_position

		self.pos.on_fill(FillEventIB(oid, 'A', SMART, 300, BUY, 10.01))

		assert self.pos.open_trade.quantity == 300
		assert self.pos.open_trade.total_quantity == 500

		self.pos.on_fill(FillEventIB(oid, 'A', SMART, 200, BUY, 10.01))
		assert self.pos.open_trade.quantity == 500
		assert self.pos.open_trade.total_quantity == 500

		self.pos._generate_signal(SHORT, 'normal')
		orders = list(self.pos.generate_orders(10000))

		oid = orders[0][0].id
		self.pos.confirm_order(orders[0][0])
		self.pos.on_fill(FillEventIB(oid, 'A', SMART, 100, SELL, 9.99))
		trade = self.pos.trades[self.pos.trade_mapper[oid]]
		assert self.pos._open_trade is None
		assert trade.total_quantity == 0
		assert trade.quantity == 400

		self.pos.on_fill(FillEventIB(oid, 'A', SMART, 400, SELL, 9.98))
		assert not self.pos.has_position

		oid2 = orders[1][0].id
		self.pos.confirm_order(orders[1][0])
		trade2 = self.pos.trades[self.pos.trade_mapper[oid2]]
		assert self.pos._open_trade == trade2.id
		assert trade.total_quantity == 0
		assert trade.quantity == 0
		assert trade.is_closed
		assert trade.id not in self.pos.trades
		# assert trade in self.pos.closed_trades

		self.pos.on_fill(FillEventIB(oid2, 'A', SMART, 400, SELL, 9.98))
		assert trade2.total_quantity == 500
		assert trade2.quantity == 400
