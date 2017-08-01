import logging, time

from threading import Thread
from abc import ABCMeta, abstractmethod
from bson import ObjectId
from blinker import signal

logger = logging.getLogger('Base')


class BaseConsumer(metaclass=ABCMeta):
	"""Base message consumer & publisher that
	- takes a list of subscriptions parameters to consume
		from a different thread
	- publish message for other consumers to consume
	"""
	def __init__(self, comp_type, required=[]):
		if comp_type.upper() not in ['MONITOR','EXE','FEED','STGY']:
			raise ValueError('Given `comp_type` is not correct.')

		self._comp_type = comp_type
		self.status = 'INIT'
		self.required = {r.lower(): False for r in required}
		self.id = ObjectId()

		self._subscriptions()
		self._setup()


	@property
	def group(self):
		return self._comp_type.lower()

	def __del__(self):
		self._stop()

	@abstractmethod
	def subscriptions(self):
		raise NotImplementedError()


	def _subscriptions(self):
		signal('get_comp').connect(self._on_get_comp, sender=self.id)

		subs = self.subscriptions()
		if subs:
			for key, sender, hdl in subs:
				signal(key).connect(hdl, sender=sender)
	
	
	def setup(self):
		# wait a bit for queue is setup before sending out messages
		time.sleep(1)
		self.basic_status_publish('SETUP')
		
		for k in self.required:
			self.basic_publish('reg-{}'.format(k), sender=self.id)

		while not all(self.required.values()):
			time.sleep(1)

		self.basic_status_publish('RUNNING')


	def _setup(self):
		"""Before everything, we need to setup the environment
		"""
		try:
			thread = Thread(target=self.setup)
			thread.start()
			thread.join()
		# if something happends here excep what we want, stop it
		except Exception as exc:
			raise exc
		except (SystemExit, KeyboardInterrupt) as exc:
			pass


	def _stop(self):
		self.basic_status_publish('STOPPED')


	def basic_publish(self, key, sender=None, **kws):
		""" Publish messages through the default settings
		"""
		body_ = {'group': self.group}
		if kws:
			body_.update(kws)

		if not sender:
			sender_ = self.id
		else:
			sender_ = sender

		return signal(key).send(sender_, body=body_)


	def basic_status_publish(self, status=None, **kws):
		""" Convinent method for publishing Status
		"""
		if status is not None:
			self.status = status
			S = status
		else:
			S = self.status

		body = {'status': S}
		body.update(kws)

		return self.basic_publish('status', **body)


	def _on_get_comp(self, oid, body):
		return self
