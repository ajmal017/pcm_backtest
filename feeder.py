import logging, pandas as pd, numpy as np

from abc import abstractmethod
from kombu import binding
from functools import lru_cache
from collections import Counter, defaultdict, deque
from bson import ObjectId
from tqdm import tqdm
from blinker import ANY

from . import conf as CONF
from .conf import DEFAULT_FREQ
from .base import BaseConsumer
from .event import MarketEvent, Tick
from pcm_pipe.backtest import TickerPipeline
from pcm_pipe.conn import BacktestDB

logger = logging.getLogger('Feeder')



class Feeder(BaseConsumer):
	"""An abstract class providing an interface for all subsequent (inherited)
	data handlers (both live and historical)

	The goal of a (derived) DataHandler object is to output a generated
	set of bars (OLHCVI) for each symbol requested
	"""
	def __init__(self):
		self.books = {}
		super().__init__(comp_type='FEED', required=[])

	
	def subscriptions(self):
		return [
			('reg-feed', ANY, self.on_reg),
			('dereg-feed', ANY, self.on_dereg),
			('warmup', ANY, self.on_warmup),
			('next', ANY, self.on_next),
		]
		

	def on_reg(self, oid, body):
		if body['group'] == 'stgy':
			if oid not in self.books:
				self.books[oid] = DataBook(self, oid)
			else:
				pass

		self.basic_publish('ack-reg-feed', sender=oid)


	def on_dereg(self, oid, body):
		if body['group'] == 'stgy':
			try:
				# no need to clean up query, it has garbage collection
				self.books.pop(oid)
			except KeyError:
				pass

		self.basic_publish('ack-dereg-feed', sender=oid)


	def on_warmup(self, stgy_oid, body):
		try:
			self.books[stgy_oid].on_warmup()
		except KeyError:
			pass  # early EOD, we then ignore it then


	def on_next(self, stgy_oid, body):
		try:
			self.books[stgy_oid].on_next()
		except KeyError:
			pass  # early EOD, we then ignore it then



class DataBook:
	def __init__(self, app, stgy_oid):
		self.app = app
		self.stgy_oid = stgy_oid
		self.bars = defaultdict(list)
		self.counter = Counter()
		self.start_time = None

		self.get_stgy_info()
		self.read_data()


	def get_avaliable_period(self, start, end):
		"""With given Starting and Ending datetime from Strategy
		- need to figure about the actural avaliable start and end
		- It will need to consider all provided symbols
		
		Parameter:
		----------
		start (datetime): The desired starting date
		end (datetime): The desired ending date

		Return:
		-------
		The actual avaliable: start (Timestamp), end (Timestamp)
		"""
		# for each symbol we want avalaible historical date range
		with BacktestDB.session_scope() as sess:
			for pipe in self.pipes:
				s_start, s_end = pipe.avaliable_period(sess=sess)

				# get the period that all symbols have data avaliable
				start = max(s_start, start)
				end = min(s_end, end)

		return start, end


	def get_stgy_info(self):
		"""Upon creating the DataBook, we need information from the strategy
		- The frequency of data that the strategy is looking for
		- how long the wamrup period would be
		- list of symbol for creating data feed
		- starting and ending timestamp for backtesting
		"""
		stgy = self.app.basic_publish('get_comp', sender=self.stgy_oid)[0][1]

		self.freq = stgy.freq
		self.symbol_list = stgy.symbol_list
		self.pipes = [TickerPipeline(t) for t in self.symbol_list]
		self.warmup = stgy.warmup

		# get number of bars to aggregate for Strategy data
		# if its 1 -> then Exe and Strategy uses same frequency
		self.num_agg = int(DEFAULT_FREQ.one_day / self.freq.one_day)

		# get the real start end range, diff symbol have diff avaliability
		self.start, self.end = self.get_avaliable_period(stgy.start_dt, stgy.end_dt)
		

	def read_data(self):
		"""Create mongo query to read in data stream for each symbols
		- It uses `Contract` ORM's function to read in bars with agg features
		"""
		with BacktestDB.session_scope() as sess:
			stream = {}
			for pipe in self.pipes:
				data = pipe.get_data(sess=sess, start=self.start, end=self.end)
				stream[pipe.ticker_id] = data.iterrows()
			self.stream = stream


	def get_new_market(self):
		data = {}
		for symbol in self.symbol_list:
			ts, bar = next(self.stream[symbol])

			tick = Tick(
				timestamp=ts,
				open=float(bar['open']),
				close=float(bar['close']),
				high=float(bar['high']),
				low=float(bar['low']),
				volume=int(bar['volume']),
				# trades=int(bar['trades']),
				# wap=float(bar['wap']),
			)

			data[symbol] = tick
			self.bars[symbol].append(tick)
		return MarketEvent(data=data)


	def on_next(self):
		"""Upon Next request, it will prepare the next bar and publish
		Handler for `next.stgy_name` event

		Parameter:
		----------
		is_stgy (bool): A flag that indicates
			whether Executer is calling next or its the strategy

		Theory:
		-------
		- During Backtesting, both the Strategy and executor would
			requir data feed. But the executor would listen to the same key
			as same as wit the strategy's key, with (probability) a 
			different frequency

		Thus, we want to avoid and make sure below things:
			- avoid duplicate feed send to executor by making sure both
				strategy and executor asks for next data.

				if they both listen for the same frequency, its alright,
				its handled on executor end using a queue. Then here we
				only send the feed once

			- make sure executor get the bar first before the strategy
				the orders that the strategy published should be filled
				using next bar, not this one

			- When the data feed ends, make sure publish EOD Event
		"""
		while True:
			end = False

			while not end:
				try:
					# fire market event for the executor first
					# filling the old orders first
					market = self.get_new_market()
					# logger.info(
					# 	'Publish Tick at Freq={} for Strategy={}'
					# 	.format(DEFAULT_FREQ, self.stgy_oid)
					# )
					self.app.basic_publish(
						'tick', sender=self.stgy_oid,
						ticks=market, freq=DEFAULT_FREQ
					)

				except StopIteration:
					# no more data, send out End-Of-Data Event
					logger.info('Publish End-Of-Data')
					self.app.basic_publish('eod', sender=self.stgy_oid)
					return

				# check to see if we need to send out event for strategy
				# if num_agg is 1, then feed to executor will always go to stgy too
				if self.num_agg != 1:
					end = self.stgy_market(market, state='real')
				else:
					end = True


	def stgy_market(self, market, state='warmup'):
		if self.start_time is None:
			self.start_time = market.timestamp

		need_agg = (
			market.timestamp - self.start_time 
			>= self.freq.offset - DEFAULT_FREQ.offset
		)
		if need_agg or market.end_of_day:
			market = MarketEvent(
				data={
					symbol: agg_bars(bars)
					for symbol, bars in self.bars.items()
				}
			)
			logger.info(
				'Publish Tick at ts={} Freq={} for Strategy={}'
				.format(market.timestamp, self.freq, self.stgy_oid)
			)
			self.app.basic_publish(
				'tick', sender=self.stgy_oid,
				ticks=market, freq=self.freq
			)
			# reset counter here
			if state != 'warmup':
				self.counter.clear()

			self.bars.clear()
			self.start_time = None
			return True
		return False


	def on_warmup(self):
		"""Prepare and Publish Warmup Data for the strategy
		Handler for `warmup.stgy_name` event
		
		Theory:
		-------
		- it will use heruistic lookback to find sure enough warmup data
			then we adjust to the amount we really need.
			Precisly calculate lookback period is expensive

		- Then it will create MarketEvent out of warmup data
			then publish them to strategies at correct frequency
		"""
		# getting the precise lookback data is expensive
		# we do it greedy, to pull for sure enough amount of data
		if self.warmup == 0: return

		start = self.start - pd.DateOffset(days=self.warmup/self.freq.one_day*2)
		end = self.start - pd.DateOffset(seconds=1)
		bar_size = DEFAULT_FREQ.bar_size

		try:
			with BacktestDB.session_scope() as sess:
				data = []
				for pipe in self.pipes:
					chunk = pipe.get_data(sess, start, end)
					# then here we adjust for enough bars to warmup
					chunk['symbol'] = pipe.ticker_id
					chunk = (
						chunk
						.sort_values('timestamp')
						.set_index(['timestamp', 'symbol'])
					)
					data.append(chunk)
		except KeyError:  # no data at all, so can't find the column
			return  # so we simply stop warming up

		need_bars = int(
			self.warmup / np.ceil(self.freq.one_day)
			* DEFAULT_FREQ.one_day
			# leave last warmup bar for new data
			- (DEFAULT_FREQ.one_day / self.freq.one_day)
		)
		data = pd.concat(data).unstack(level=1).dropna().tail(need_bars)

		# start publishing ticks to strategy for warming up
		for ts, row in data.iterrows():
			ticks = {}
			for symbol, bar in row.swaplevel().groupby(level=0):
			    tick = Tick(timestamp=ts, **bar[symbol])
			    ticks[symbol] = tick
			    self.bars[symbol].append(tick)

			market = MarketEvent(data=ticks)
			self.stgy_market(market, state='warmup')

		# make sure reset counter here again, just makeing sure
		self.bars.clear()
		self.start_time = None


def agg_bars(bars):
	bars = np.asarray(bars)

	timestamp = bars[-1, 0]  # 0 -> timestamp
	open = bars[0, 1]  # 1 -> open
	close = bars[-1, 2]  # 2 -> close
	high = np.max(bars[:, 3])  # 3 -> high
	low = np.min(bars[:, 4])  # 4 -> low
	volume = np.sum(bars[:, 5])  # 5 -> volume

	return Tick(
		timestamp=timestamp,
		open=open, close=close, high=high, low=low,
		volume=volume,
	)
