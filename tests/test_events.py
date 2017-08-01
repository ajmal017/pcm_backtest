import pandas as pd, numpy as np

from mock import patch, PropertyMock
from nose.tools import raises
from datetime import datetime
from pandas import Timestamp
from bson import ObjectId

from pcm_backtest.event import (
	Event, MarketEvent, Tick, SignalEventPct, OrderEvent,
	FillEvent, FillEventIB
)
from pcm_backtest.conf import (
	FILL_TYPE, SIGNAL_TYPE, ORDER_TYPE,
	BUY, SELL, LONG, SHORT, EXIT, MARKET, SIGNAL, ORDER, FILL,
	MKT, LMT, SMART, LOCAL_TZ
)


class TestEvent:
	@classmethod
	@patch.multiple(Event, __abstractmethods__=set())
	def setup_class(self):
		self.evt = Event()


	def test_slot_size(self):
		assert len(self.evt.__slots__) == 1


	@raises(NotImplementedError)
	def test_as_dict_abstract(self):
		self.evt.as_dict()


	@raises(NotImplementedError)
	def test_from_dict_abstract(self):
		self.evt.from_dict()


	@patch('pcm_backtest.event.core.Event.as_dict')
	@patch('pcm_backtest.event.core.json.dumps')
	def test_as_json(self, mock_dump, mock_as_dict):
		mock_as_dict.return_value = {}

		self.evt.as_json()
		mock_dump.assert_called_once_with({})


	@patch('pcm_backtest.event.core.Event.from_dict')
	@patch('pcm_backtest.event.core.json.loads')
	def test_from_json(self, mock_loads, mock_from_dict):
		self.evt.from_json("{'a': 100}")
		mock_loads.assert_called_once()
		mock_from_dict.assert_called_once()



class TestMarketEvent:
	@classmethod
	def setup_class(self):
		self.evt = MarketEvent({'a':
			Tick(Timestamp('2017-06-05T23:50:41'), 10,10,10,10,1000)
		})


	def test_slot_size(self):
		assert len(self.evt.__slots__) == 2


	def test_type(self):
		print(self.evt.type)
		assert self.evt.type is MARKET


	@patch('pandas.Timestamp.isoformat')
	def test_as_dict(self, mock_isoformat):
		data = self.evt.as_dict()

		mock_isoformat.assert_called_once()
		assert 'data' in data
		assert data['event_type'] == 'market'


	def test_from_dict(self):
		data = self.evt.as_dict()
		new_evt = MarketEvent.from_dict(**data['data'])

		assert new_evt == self.evt


	def test_local_ts(self):
		assert str(self.evt.local_ts.tz) == str(LOCAL_TZ)


	@patch('pandas.Timestamp.time')
	def test_time(self, mock_time):
		self.evt.time
		mock_time.assert_called_once()


	def test_end_of_day(self):
		assert self.evt.end_of_day


	@patch('pandas.Timestamp.dayofweek', new_callable=PropertyMock)
	def test_end_of_week(self, mock_dow):
		mock_dow.return_value = 0
		assert not self.evt.end_of_week
		mock_dow.assert_called_once



class TestSignalEvent:
	@classmethod
	def setup_class(self):
		self.evt = SignalEventPct('AAPL', LONG, 1)
		self.evt2 = SignalEventPct('AAPL', SHORT, 0.2505)


	def test_slot_size(self):
		assert len(self.evt.__slots__) == 4


	def test_init(self):
		assert isinstance(self.evt.id, ObjectId)
		assert isinstance(self.evt.timestamp, Timestamp)
		assert self.evt.symbol == 'AAPL'


	def test_sort(self):
		assert self.evt > self.evt2


	def test_target_qty(self):
		assert np.isclose(self.evt.target_qty(10, 10000), 1000)
		assert np.isclose(self.evt2.target_qty(10, 10000), -250)


	def test_as_dict(self):
		data = self.evt.as_dict()

		assert 'data' in data
		assert data['event_type'] == 'signal_pct'


	def test_from_dict(self):
		data = self.evt.as_dict()
		new_evt = SignalEventPct.from_dict(**data['data'])

		assert new_evt == self.evt



class TestOrderEvent:
	@classmethod
	def setup_class(self):
		self.evt = OrderEvent('AAPL', MKT, 100, BUY)
		self.evt2 = OrderEvent('AAPL', MKT, 100, SELL)


	def test_init(self):
		assert isinstance(self.evt.id, ObjectId)
		assert isinstance(self.evt.timestamp, Timestamp)
		assert self.evt.quantity == 100
		assert self.evt.symbol == 'AAPL'


	def test_as_dict(self):
		data = self.evt.as_dict()

		assert 'data' in data
		assert data['event_type'] == 'order'


	def test_from_dict(self):
		data = self.evt.as_dict()
		new_evt = OrderEvent.from_dict(**data['data'])
		print(new_evt.id, self.evt.id)
		assert new_evt == self.evt


	def test_sort(self):
		assert self.evt > self.evt2



class TestFillEvent:
	@classmethod
	@patch.multiple(FillEvent, __abstractmethods__=set())
	def setup_class(self):
		self.evt = FillEvent(
			order_id=ObjectId(b'123-123-1234'),
			symbol='AAPL', exchange=SMART, quantity=100,
			fill_type=BUY, fill_cost=10, commission=0.1
		)
		self.evt2 = FillEvent(
			order_id=ObjectId(b'123-123-1234'),
			symbol='AAPL', exchange=SMART, quantity=100,
			fill_type=SELL, fill_cost=10, commission=0.1
		)


	@patch('pcm_backtest.event.FillEvent.calculate_commission')
	@patch.multiple(FillEvent, __abstractmethods__=set())
	def test_init(self, mock_comm):
		evt = FillEvent(
			order_id=ObjectId(b'123-123-1234'),
			symbol='AAPL', exchange=SMART, quantity=100,
			fill_type=BUY, fill_cost=10,
		)

		assert isinstance(evt.id, ObjectId)
		assert isinstance(evt.timestamp, Timestamp)
		assert evt.quantity == 100
		assert evt.symbol == 'AAPL'
		mock_comm.assert_called_once()


	def test_sort(self):
		assert self.evt > self.evt2



class TestFillEventIB:
	@classmethod
	def setup_class(self):
		self.evt = FillEventIB(
			order_id=ObjectId(b'123-123-1234'),
			symbol='AAPL', exchange=SMART, quantity=100,
			fill_type=BUY, fill_cost=10,
		)
		self.evt2 = FillEventIB(
			order_id=ObjectId(b'123-123-1234'),
			symbol='AAPL', exchange=SMART, quantity=100,
			fill_type=SELL, fill_cost=10,
		)
		self.evt3 = FillEventIB(
			order_id=ObjectId(b'123-123-1234'),
			symbol='AAPL', exchange=SMART, quantity=100,
			fill_type=SELL, fill_cost=0.1,
		)


	def test_sort(self):
		assert self.evt > self.evt2


	def test_as_dict(self):
		data = self.evt.as_dict()

		assert 'data' in data
		assert data['event_type'] == 'fill_ib'


	def test_from_dict(self):
		data = self.evt.as_dict()
		new_evt = FillEventIB.from_dict(**data['data'])

		assert new_evt == self.evt


	def test_calculate_commission(self):
		assert np.isclose(self.evt.commission, 0.5)


	def test_calculate_max_comm(self):
		assert np.isclose(self.evt3.commission, 0.05)
