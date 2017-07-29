from mock import patch, call, MagicMock
from nose.tools import assert_raises
from bson import ObjectId
from kombu import binding, Queue

from pcm.base import BaseConsumer, EventConsumer, CompConsumer
from pcm.errors import DeadProducer
from pcm.util import get_temp_queue, compress_data
from pcm.conf import AMQP


class FakeRegister:
	def __init__(self):
		self.comp_type = 'FAKE'
		self.id = ObjectId(b'123-123-1234')
		self.status = 'INIT'
		self.env_type = 'UNITTEST'

	def save(self):
		pass


class FakeBaseConsumer(BaseConsumer):
	def subscriptions(self):
		exg = self.trading_exchange
		return [{
			'queues': get_temp_queue(
				self, auto_delete=True, durable=False,
				bindings=[
					binding(exg, 'reg-exe'),
					binding(exg, 'dereg-exe'),
					binding(exg, 'ack-reg-feed.{}'.format(self.name)),
					binding(exg, 'order.*'),
				]
			),
			'handlers': self.on_msg,
			'kws': dict(no_ack=False, accept=['msgpack'], prefetch_count=100)
		}]

	def on_msg(self, body, message):
		pass



class TestBaseConsumer:
	@patch('pcm.base.time')
	@patch('kombu.messaging.Producer.publish')
	@patch('pcm.base.EventConsumer')
	def test_init(self, mock_consumer, mock_publish, mock_time):
		base = FakeBaseConsumer(FakeRegister(), required=[])
		assert base.status == 'RUNNING'
		assert base._recv is not None
		assert base.producer.is_alive()
		mock_time.sleep.assert_called_once_with(3)
		mock_consumer.return_value.run.assert_called_once()

		base.basic_status_publish()
		call_dict = dict(
			exchange=base.trading_exchange, declare=[base.trading_exchange],
			routing_key=base.status_key, 
			serializer='msgpack', priority=0,
			headers={'group': base.group, 'name': base.name},
		)
		base._msg_queue.join()

		call_dict['body'] = compress_data({'status': 'SETUP'})
		assert mock_publish.call_args_list[0] == call(**call_dict)
		
		call_dict['body'] = compress_data({'status': 'RUNNING'})
		assert mock_publish.call_args_list[1] == call(**call_dict)

		call_dict['body'] = compress_data({'status': 'RUNNING'})
		assert mock_publish.call_args_list[2] == call(**call_dict)
		assert base._msg_queue.empty()

		base.__del__()
		assert base.status == 'STOPPED'
		assert not base.producer.is_alive()
		assert not base.consumer.is_alive()

		base.__del__()  # try to stop, when it is already stopped
		assert base.status == 'STOPPED'
		assert not base.producer.is_alive()
		assert not base.consumer.is_alive()

		assert_raises(DeadProducer, base.basic_publish, None)



class TestEventConsumer:
	@patch('kombu.connection.Connection')
	def test_init(self, mock_conn):
		exg = AMQP.exchanges['UNITTEST']
		sub = [{
			'queues': Queue(),
			'handlers': lambda body, message: print(100),
			'kws': dict(no_ack=False, accept=['msgpack'], prefetch_count=100)
		}]
		worker = EventConsumer(mock_conn.return_value, sub)
		conn = worker.connection
		consumers = worker.get_consumers(conn.Consumer, conn.channel())
	
		conn.Consumer.assert_called_once()



class TestCompConsumer:
	def setup(self):
		CompConsumer.__bases__ = (MagicMock, )


	@patch('pcm.base.Register')
	@patch('pcm.base.CompConsumer.start')
	def test_init(self, mock_start, mock_reg):
		comp = CompConsumer('FAKE', env_type='UNITTEST')

		mock_reg.assert_called_once_with(env_type='UNITTEST', comp_type='FAKE')
		mock_start.assert_called_once()
