from mock import MagicMock, patch, create_autospec, PropertyMock
from math import ceil
from datetime import datetime
from unittest import TestCase

from pcm.strategy.core import BaseStrategy
from pcm.orm.reg import StgyRegister
from pcm.conf import H1


class FakeContract:
	def __init__(self, symbol):
		self.symbol = symbol

	@property
	def perm_tick(self):
		return id(self.symbol)


class FakePos:
	def __init__(self, symbol, pct_portfolio, rebalance, hard_stop):
		self.symbol = FakeContract(symbol)
		self.pct_portfolio = pct_portfolio
		self.rebalance = rebalance
		self.hard_stop = hard_stop



class TestBaseStrategy(TestCase):
	def setUp(self):
		time_patch = patch('pcm.base.time')
		publish_patch = patch('kombu.messaging.Producer.publish')
		setup_patch = patch('pcm.base.BaseConsumer._setup')
		event_consumer_patch = patch('pcm.base.EventConsumer')
		position_patch = patch('pcm.strategy.core.Position')
		register_patch = patch('pcm.strategy.core.StgyRegister.register')
		wait_patch = patch('pcm.strategy.core.BaseStrategy._wait_stop')
		self.addCleanup(time_patch.stop)
		self.addCleanup(publish_patch.stop)
		self.addCleanup(event_consumer_patch.stop)
		self.addCleanup(position_patch.stop)
		self.addCleanup(register_patch.stop)
		self.addCleanup(setup_patch.stop)
		self.addCleanup(wait_patch.stop)

		self.mock_time = time_patch.start()
		self.mock_publish = publish_patch.start()
		self.mock_event_consumer = event_consumer_patch.start()
		self.mock_pos = position_patch.start()
		self.mock_reg = register_patch.start()
		self.mock_setup = setup_patch.start()
		self.mock_wait = wait_patch.start()

		BaseStrategy.__abstractmethods__ = set()
		self.base_stgy = BaseStrategy

		self.params = dict(
			symbol_list=['AAPL', 'GOOG'], allocation=100000, freq=H1,
			positions={
				'AAPL': dict(pct_porfolio=0.5, hard_stop=0, rebalance=0),
				'GOOG': dict(pct_porfolio=0.5, hard_stop=0.1, rebalance=1),
			},
			fixed_allocation=True, warmup=5, env_type='UNITTEST',
			start=datetime(2015,1,1), end=datetime(2016,1,1)
		)
		self.mock_reg.return_value.pos = [
			FakePos('AAPL', 0.5, 0, 0),
			FakePos('GOOG', 0.5, 1, 0.1),
		]
		self.mock_reg.return_value.env_type = 'UNITTEST'
		self.mock_reg.return_value.status = 'INIT'
		self.mock_reg.return_value.start = datetime(2015,1,1)
		self.mock_reg.return_value.end = datetime(2016,1,1)


	def test_init(self):
		stgy = self.base_stgy(**self.params)
		self.mock_reg.assert_called_once_with(**self.params)

		assert self.mock_pos.call_count == 2
		assert stgy.t == 0
		assert stgy.required == {'feed': False, 'exe': False}

		stgy.__del__()
		stgy._msg_queue.join()


	@patch('pcm.strategy.core.tqdm')
	def test_start(self, mock_tqdm):
		self.mock_reg.return_value.status = 'RUNNING'
		stgy = self.base_stgy(**self.params)

		stgy.start()
		stgy.__del__()
		stgy._msg_queue.join()

		self.mock_time.assert_not_called()
		mock_tqdm.assert_called_once()
		assert self.mock_wait.call_count == 2
		assert self.mock_publish.call_count == 3

		assert 'warmup.' in str(self.mock_publish.call_args_list[0])
		assert 'next.' in str(self.mock_publish.call_args_list[1])


	