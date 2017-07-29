import pandas as pd
import pandas_market_calendars as mcal

from tqdm import tqdm
from collections import namedtuple

from pcm.orm.ticker import Ticker
from pcm.orm.market import (
	Market, News, Fundamental, Derivatives, Underlying
)
from pcm.orm.dump import DataDump
from pcm.orm.contract import Contract
from pcm.orm.util import (
	t_to_rth_open, in_rth, clean_timestamp, round_timestamp
)
from pcm.conf import GLOBAL_TZ, LOCAL_TZ



Period = namedtuple('Period', ['start', 'end'])


class TickerPipeline:
	def __init__(self, perm_tick):
		self.ticker = Ticker.get_ticker(perm_tick)


	def markets(
		self, resolution='1 min',
		start=None, end=None, rth=True, minimal=True
	):
		"""Return a Queryset of Contract's Market Chunk docs

		Note:
		-----
		- all 'bars' are not eager loaded for efficiency reading
		"""
		cond = dict(ticker=self.ticker, resolution=resolution, rth=rth)
		if start and end:
			cond['timestamp__lte'] = end
			cond['timestamp__gte'] = start

		qu = Market.objects(**cond).order_by('timestamp')
		
		if minimal:
			qu = qu.only('id')

		return qu


	def avaliable_period(self, resolution='1 min', rth=True):
		"""Get the beginning and ending timestamp
		based this Contract's avaliable histroical data
		
		Parameter:
		----------
		resolution (str): The resolution Market Chunk to lookup

		Return:
		-------
		start (pd.Timestamp), end (pd.Timestamp)
		"""
		ms = self.markets(resolution=resolution, rth=rth)
		output = ms.aggregate({
			'$group': {
				'_id': None,
				'end': {'$max': '$timestamp'},
				'start': {'$min': '$timestamp'},
			}
		}).next()

		return Period(start=output['start'], end=output['end'])


	def get_market_date_range(self, start, end, exchange, resolution='1 min'):
		# get market calendar
		nyse = mcal.get_calendar(exchange)
		schedule = nyse.schedule(start, end-pd.DateOffset(days=1))

		# get correct frequency str
		n, offset = resolution.split()
		if 'min' in offset:
			offset = 'T'
		else:
			offset = offset[0].upper()
		freq = '{} {}'.format(n, offset)

		# get the true date range of timestamp
		rng = mcal.date_range(
			schedule, freq, force_close=True, closed='right'
		).tz_convert(None) + pd.DateOffset(seconds=-1)
		return rng



	def missing_date(self, start, end, resolution='1 min', exchange='NYSE'):
		# getting contract avalaible timestamps
		data_sr = pd.Series(
			pd.Series(
				self.markets(start=start, end=end, resolution=resolution)
				.distinct('timestamp')
			)
			.dt.tz_localize(GLOBAL_TZ)
			.dt.tz_convert(LOCAL_TZ)
			.dt.date
		).value_counts()

		# getting true market timestamps
		rng = self.get_market_date_range(start, end, exchange, resolution)
		true_sr = pd.Series(rng.date).value_counts()

		output = true_sr - data_sr
		return output[output!=0].sort_values()



	def get_agg_bars(
		self, start, end, rth=True,
		resolution='1 min', agg_bar_size='10 mins',
	):
		"""Get A Mongo Result Set on Aggregated Market Bars
		- by default all bars will be in RTH
		- the calculation will be done based on downsampling method
			from higher frequency bars

		Parameters:
		-----------
		start (datetime): The starting datetime of the query (inclusive)
		end (datetime): The ending datetime of the query (inclusive)
		resolution (str): The resolution Market Chunk to lookup for aggregation
		agg_bar_size (str): The desired resolution for output

		Return:
		-------
		Mongo Result Set Cursor 
		"""
		def interval(n):
			return {  # by n interval using mode
				"$subtract": ['$t', {"$mod": ["$t", n]}],
			}

		def get_agg_bars_key(resolution):
			n, unit = resolution.lower().split()
			n = int(n)
			key = {
				'year': {'$year': '$timestamp'},
				'month': {'$month': '$timestamp'},
			}
			
			# check day group first
			if 'day' in unit:
				key['day'] = {'$dayOfMonth': '$timestamp'}
				key['interval'] = interval(n * 390 * 60)
				return key
			else:
				key['day'] = {'$dayOfMonth': '$timestamp'}
				
			# then hourly group
			if 'hour' in unit:
				key['interval'] = interval(n * 60 * 60)
				return key

			elif 'min' in unit:  # then minutely group
				key['interval'] = interval(n * 60)
				return key
				
			# finally sec we abort
			if 'sec' in unit:
				raise ValueError('Currently sec level aggration is not avalaible')
			return key

		start_ = clean_timestamp(start)
		end_ = clean_timestamp(end)

		res = (
			self.markets(resolution, start_, end_, rth=rth, minimal=True)
			.aggregate(
				# ascendingly sorting for later grouping using $first & $last
				{'$sort': {'timestamp': 1}},
				{ 
					'$project': {  # get bars data out
						'_id': 0,
						't': 1, 'timestamp': 1, 'rth': 1,
						'open': '$underlying.open',
						'high': '$underlying.high',
						'low': '$underlying.low',
						'close': '$underlying.close',
						'volume': '$underlying.volume',
					}
				},
				{
					'$group': {  # group by time chunk
						'_id': get_agg_bars_key(agg_bar_size),
						'timestamp': {'$max': '$timestamp'},
						'open': {'$first': '$open'},
						'high': {'$max': '$high'},
						'low': {'$min': '$low'},
						'close': {'$last': '$close'},
						'volume': {'$sum': '$volume'},
					}
				},
				{
					'$project': {
						'_id': 0, 'timestamp': 1, 'volume': 1,
						'open': 1, 'high': 1, 'low': 1, 'close': 1,
					}
				},
				{'$sort': {'timestamp': 1}},
				allowDiskUse=True,  # large sorting will need this
				batchSize=30000,  # to prevent cursor dieing
			)
		)
		return res



class MarketFactory:
	def __init__(self, resolution='1 min'):
		self.resolution = resolution


	def new_markets(self, ticker, batch_size=10000, force=True):
		"""Insert not exists Markets from Dump
		"""
		# locating the right ticker
		ticker_ = Ticker.get_ticker(ticker)
		ticker_id = ticker_.id

		# if force, we delete all the previous markets
		if force:
			Market.objects(ticker=ticker_, resolution=self.resolution).delete()

		# query out dumps for processing
		qu = DataDump.objects(ticker=ticker_id, resolution=self.resolution)
		total, cur = DataDump.raw_markets(query=qu)
		cur = cur.batch_size(batch_size * 5)
		
		pbar = tqdm(total=total)
		ms = []
		used = 0

		for res in cur:
			ts_ = round_timestamp(clean_timestamp(res['_id']), self.resolution)
			m = Market(
				ticker=ticker_,
				resolution=self.resolution,
				timestamp=ts_,
				t=t_to_rth_open(ts_),
				# TODO: needs to take in account holiday and half-days
				rth=in_rth(ts_),
			)
			m = self.update_market(m, res['show'], res['data'])
			
			ms.append(m)
			used += len(res['ids'])

			if len(ms) % batch_size == 0:
				Market.objects.insert(ms)
				pbar.update(used)
				ms = []
				used = 0

		if ms:
			Market.objects.insert(ms)
			pbar.update(pbar.total - pbar.n)

		# in case the cursor dead, we create another one for updating
		# atomic update on used
		qu = DataDump.objects(ticker=ticker_id, resolution=self.resolution)
		qu.update(used=True)
		pbar.close()


	def push_to_market(self, ticker, batch_size=10000):
		"""Exhasutive Pushing for all not used dumps
		- for each, it will upsert the dump to market
		"""
		ticker_ = Ticker.get_ticker(ticker)
		qu = DataDump.objects(
			ticker=ticker_.id, resolution=self.resolution, used__ne=True
		)
		total, cur = DataDump.raw_markets(query=qu)
		cur = cur.batch_size(batch_size * 5)

		pbar = tqdm(total=total)
		used = 0
		for res in cur:
			ts_ = round_timestamp(clean_timestamp(res['_id']), self.resolution)
			m = Market.get_market(ticker_, ts_, resolution=self.resolution)

			m = self.update_market(m.reload(), res['show'], res['data'])
			m.save()
			used += len(res['ids'])

			if used % batch_size == 0:
				pbar.update(batch_size)
				used = 0

		if used:
			pbar.update(pbar.total - pbar.n)

		# in case the cursor dead, we create another one for updating
		# atomic update on used
		DataDump.objects(
			ticker=ticker_.id, resolution=self.resolution, used__ne=True
		).update(used=True)
		pbar.close()
	

	def update_market(self, market, show_list, data_list):
		for i, show in enumerate(show_list):
			if show in ['TRDS', 'IVOL', 'HVOL']:
				if market.underlying:
					for k, v in data_list[i].items():
						setattr(market.underlying, k, v)
				else:
					u = Underlying(**data_list[i])
					market.underlying = u
			elif show == 'OPTS':
				pass  # placeholder
			elif show == 'NEWS':
				pass  # placeholder
			elif show == 'FUND':
				pass  # placeholder

		return market


	def fix_missing_bars(self, ticker, exchange='NYSE', batch_size=10000):
		ticker_ = Ticker.get_ticker(ticker)
		pipe = TickerPipeline(ticker_.perm_tick)

		# current DB avalaible period
		period = pipe.avaliable_period()
		start = period.start.date()
		end = period.end.date() + pd.DateOffset(days=1)

		# getting market calcener scheudle
		correct_rng = pipe.get_market_date_range(
			start=start, end=end, exchange=exchange
		)
	
		# pulling out all the data
		df = pd.DataFrame(list(
			pipe.get_agg_bars(
				start=start, end=end,
				agg_bar_size=self.resolution
			)
		)).sort_values('timestamp').set_index('timestamp')

		# get missed data and filling backword
		missed = df.ix[correct_rng].isnull().all(axis=1)
		missed = missed[missed]
		missed_data = df.ix[correct_rng].fillna(method='bfill').ix[missed.index]
		
		dds = []
		pbar = tqdm(total=missed.shape[0])
		for ts, d in missed_data.iterrows():
			data = {
				'open': d.open,
				'high': d.high,
				'low': d.low,
				'close': d.close,
				'volume': d.volume,
			}
			dd = DataDump(
				resolution=self.resolution, show='TRDS',
				timestamp=ts, ticker=ticker_.id, data=data,
				used=False
			)
			dd.clean()
			dds.append(dd)
			pbar.update(1)
		
		if dds:
			DataDump.objects.insert(dds)
		pbar.close()
		
		self.push_to_market(ticker_.perm_tick)
