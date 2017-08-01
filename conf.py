import pytz, pandas as pd, datetime


# -----------------------------------------------------------------------------
# Timezone Configurations
# -----------------------------------------------------------------------------
LOCAL_TZ = pytz.timezone('America/New_York')
GLOBAL_TZ = pytz.utc
RTH_OPEN = datetime.time(9, 30)
RTH_CLOSE = datetime.time(16, 0)


# -----------------------------------------------------------------------------
# Main System Settings
# -----------------------------------------------------------------------------
# Signal Configuration
class LONG:
	value = 'LONG'
	sign = 1

	def __repr__(self):
		return self.value

class SHORT:
	value = 'SHORT'
	sign = -1

class EXIT:
	value = 'EXIT'
	sign = 0

SIGNAL_TYPE = LONG, SHORT, EXIT
SIGNAL_DICT = {
	'L': LONG,
	'S': SHORT,
	'E': EXIT,
}

# Order and Configuration
class MKT:
	value = 'MKT'

class LMT:
	value = 'LMT'

ORDER_TYPE = MKT, LMT
ORDER_DICT = {
	'M': MKT,
	'L': LMT,
}

# Fill configuration
class BUY:
	value = 1

class SELL:
	value = -1

FILL_TYPE = BUY, SELL
FILL_DICT = {
	1: BUY,
	-1: SELL,
}

# Event Configuration
class MARKET:
	value = 'MARKET'

class SIGNAL:
	value = 'SIGNAL'

class ORDER:
	value = 'ORDER'

class FILL:
	value = 'FILL'

EVENT_TYPE = MARKET, SIGNAL, ORDER, FILL

# Broker Exchange routing
class SMART:
	value = 'SMART'

EXCHANGE_TYPE = SMART,
EXCHANGE_DICT = {
	'S': SMART,
}


# Data Frequency
class DAILY:
	value = 'DAILY'
	queue_tag = '1D'
	bar_size = '1 day'
	one_day = 1
	N = 250
	offset = pd.Timedelta(days=1)

class H1:
	value = '1 H'
	queue_tag = ''.join(value.split())
	bar_size = '1 hour'
	one_day = 6.5
	N = DAILY.N * one_day
	offset = pd.Timedelta(hours=1)

class M30:
	value = '30 M'
	queue_tag = ''.join(value.split())
	bar_size = '30 mins'
	one_day = 13
	N = DAILY.N * one_day
	offset = pd.Timedelta(minutes=30)

class M10:
	value = '10 M'
	queue_tag = ''.join(value.split())
	bar_size = '10 mins'
	one_day = M30.one_day * 3
	N = DAILY.N * one_day
	offset = pd.Timedelta(minutes=10)

class M1:
	value = '1 M'
	queue_tag = ''.join(value.split())
	bar_size = '1 min'
	one_day = M10.one_day * 10
	N = DAILY.N * one_day
	offset = pd.Timedelta(minutes=1)


FREQ_TYPE = DAILY, H1, M30, M10, M1
DEFAULT_FREQ = M1


class Constant:
	# Rf = (0.82+2) / 100
	Rf = 0
	alpha = 0.99


# -----------------------------------------------------------------------------
# Connection Setting 
# -----------------------------------------------------------------------------
# Database Configuration
class SECMaster:
	url_pattern = '{host}:{port}'
	host = '192.168.0.32'
	port = 27017
	db = 'sec_master'
	url = url_pattern.format(host=host, port=port)
	default_tz = GLOBAL_TZ
