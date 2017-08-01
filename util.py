import inspect, lz4, pickle, pandas as pd

from binascii import a2b_uu, b2a_uu
from bson import ObjectId
from contextlib import contextmanager
from kombu import Queue
from concurrent.futures import ThreadPoolExecutor
from ibapi.contract import Contract as IBcontract

from pcm.event import EVENT_MAP
from pcm.conf import AMQP, LOCAL_TZ, GLOBAL_TZ


thread_pool = ThreadPoolExecutor(50)


def clean_timestamp(timestamp):
	"""Convert a localized timestamp to UTC & Resoultion on Second
	Centarl function to make sure all timestamp is on UTC

	Parameter:
	----------
	ts: Timestamp, can be in format `ISO` `datetime` `pd.Timestamp`
	"""
	ts = pd.Timestamp(timestamp).replace(microsecond=0)
	try:
		ts = ts.tz_convert(LOCAL_TZ)
	except TypeError:  # not tz-aware
		ts = ts.tz_localize(GLOBAL_TZ).tz_convert(LOCAL_TZ)

	return ts


def decompress_data(data):
	return pickle.loads(lz4.loads(data))


def compress_data(data):
	return lz4.compressHC(pickle.dumps(data))


@contextmanager
def amqp_conn_scope(block=True):
	"""Context manager of Rabbitmq Connection
	- connection is acquired from the poll

	Parameter
	---------
	block: bool, whether wait for the producer be avaliable
	"""
	try:
		with AMQP.ConnPool[AMQP.ConnParam].acquire(block=block) as conn:
			yield conn
	except (KeyboardInterrupt, SystemExit) as exc:
		pass
	except Exception as exc:
		raise exc


@contextmanager
def amqp_prod_scope(block=True):
	"""Context manager of Producer connection
	- producer will be created out of avaliable connection
	- connection is acquired from the pool

	Parameter
	---------
	block: bool, whether wait for the producer be avaliable
	"""
	# with amqp_conn_scope(block=block) as conn:
	try:
		with AMQP.ProdPool[AMQP.ConnParam].acquire(block=block) as prod:
			yield prod
	except (KeyboardInterrupt, SystemExit) as exc:
		pass
	except Exception as exc:
		raise exc


def get_temp_queue(consumer, **kws):
	"""Convient function to create temporary queue

	Parameter
	---------
	consumer: BaseConsumer Subclass
		to make random name distinguishble as a group
		random name is based on
		- Consumer's group name
		- Consumer's its own name
		- Random ObjectId()
	kws: any keyword arguments that pass to komby.Queue for construction

	Note:
	-----
	This naming convention ensures that each component would have
	multiple queues
	"""
	name_ = '{}.{}.{}'.format(
		consumer.group,
		consumer.name,
		str(ObjectId())
	)
	return Queue(name_, **kws)
	

def key_groups(routing_key):
	"""Central function for helping decompose routing key

	Return
	------
	group name, class name, message type
	"""
	return routing_key.split('.')


def event_from_dict(body):
	"""Convient helper function to reconstruct Event from dictionary

	Parameter
	---------
	body: dict, {'event_type':..., 'data': ...}
		- event_type: for what kind of event
			avaliable: {'market', 'order', 'fill_ib', 'signal'}
		- data: addiation data go with the event

	Return
	------
	Any Inherited instance out of Event
	"""
	return EVENT_MAP[body['event_type']].from_dict(**body['data'])


def oid_to_ascii(oid):
	return b2a_uu(oid.binary).decode('ascii')

def ascii_to_oid(string):
	return ObjectId(a2b_uu(string))
	
def funcspec(func):
	return inspect.getargsepc(func)


def as_tuple(items):
	"""Given item or items
	- it will make sure them to be wrapper by a tuple
	- it themselves is already wrapper by list or tuple, nothing happends
	
	Return
	------
	a tuple of stuffs
	"""
	if items is not None and not isinstance(items, (list, tuple)):
		return (items, )
	else:
		return items


def dollar_trunc(x, decimal=0, dollar_sign=False):
	"""Auto handling number to dollar formatting

	Parameter
	---------
	x: numric, value to turn in dollar string
	decimal: int, number of decimal to keep or add in
	dollar_sign: bool, whether to add in dollar sign '$'

	Note
	----
	if x is 0 the output will always be '-' w/o the dollar sign
	"""
	abs_x = abs(x)
	
	if abs_x!=0:
		if abs_x < 1e2:   # below one hundred
			output = ('{:,.%sf}' % decimal).format(x)
		if abs_x < 1e6:   # below one million
			output = ('{:,.%sf}K' % decimal).format(x/1e3)
		elif abs_x < 1e9: # below one billion
			output = ('{:,.%sf}M' % decimal).format(x/1e6)
		else:
			output = ('{:,.%sf}B' % decimal).format(x/1e9)
	else:
		output = '-'

	if dollar_sign:
		if output != '-':
			return '$' + output
		else:
			return output
	else:
		return output


def gen_int_id(threshold):
	"""Infinitly yields incrementing looped integer from 0"""
	i = 1
	while i <= threshold:
		yield i
		i += 1


def forex_contract(symbol):
	C = IBcontract()
	C.symbol = symbol.split('.')[0]
	C.secType = 'CASH'
	C.exchange = 'IDEALPRO'
	C.currency = symbol.split('.')[-1]
	return C


def index_contract(symbol, exg='CBOE', currency='USD'):
	C = IBcontract()
	C.symbol = symbol
	C.secType = 'IND'
	C.currency = currency
	C.exchange = exg
	return C


def stock_contract(symbol, exchange='SMART', currency='USD'):
	C = IBcontract()
	C.symbol = symbol
	C.secType = 'STK'
	C.currency = currency
	C.exchange = exchange
	return C


def future_contract(symbol, exchange='NYMEX'):
	C = IBcontract()
	C.secType = 'FUT'
	C.exchange = exchange
	C.currency = 'USD'
	C.localSymbol = symbol
	return C


def to_pandas_offset(bar_size):
	n, t = bar_size.split()
	t = t[0].upper()
	if t == 'M':
		t = 'T'
	return n+t
