from pandas import Timestamp, DateOffset
from collections import namedtuple, UserDict
from datetime import time

from pcm_backtest.conf import MARKET, GLOBAL_TZ, LOCAL_TZ, RTH_CLOSE
from .core import Event


FRIDAY = 4
Tick = namedtuple(
	'Tick',
	['timestamp','open','close','high','low','volume']
)


class MarketEvent(Event, UserDict):
	"""
	Handles the event of receiving new market update
	with correspoding bars
	"""
	__slots__ = ['data','timestamp']
	type = MARKET

	def __init__(self, data):
		self.data = data
		self.timestamp = data[tuple(data.keys())[0]].timestamp


	def as_dict(self):
		data = {}
		for k, v in self.data.items():
			tmp = list(v)
			tmp[0] = tmp[0].isoformat()
			data[k] = tmp

		return {
			'event_type': 'market',
			'data': data,
		}
	

	@classmethod
	def from_dict(cls, **kws):
		data = {}
		for k,v in kws.items():
			v[0] = Timestamp(v[0])
			data[k] = Tick(*v)
		return cls(data=data)

	
	@property
	def local_ts(self):
		try:
			ts = self.timestamp.tz_localize(GLOBAL_TZ).tz_convert(LOCAL_TZ)
		except:  # already have tz
			ts = self.timestamp.tz_convert(LOCAL_TZ)

		return ts + DateOffset(seconds=1)


	@property
	def time(self):
		return self.local_ts.time()


	@property
	def end_of_day(self):
		return self.time >= RTH_CLOSE


	@property
	def end_of_week(self):
		return self.end_of_day and self.local_ts.dayofweek >= FRIDAY
