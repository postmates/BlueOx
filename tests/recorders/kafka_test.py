import os
import random
import decimal
import datetime

import msgpack
from testify import (
    TestCase,
    setup,
    teardown,
    assert_equal)

from blueox import default_configure, KAFKA_RECORDER
from blueox import utils
from blueox import context
from blueox.recorders import kafka
from blueox.recorders import zmq


class MockKafkaProducer(object):
    last_topic = None
    last_data = None
    close_timeout = None

    def __call__(self, bootstrap_servers=None):
        self.bootstrap_servers = bootstrap_servers
        return self

    def send(self, topic, data):
        self.last_topic = topic
        self.last_data = data

    def flush(self):
        pass

    def close(self, timeout=None):
        self.close_timeout = timeout


class KafkaOverrideTestCase(TestCase):
    @teardown
    def clear_env(self):
        try:
            del os.environ['BLUEOX_OVERRIDE_KAFKA_RECORDER']
        except KeyError:
            pass

    def test_configure_no_override(self):
        default_configure()
        assert_equal(context._recorder_function, zmq.send)

    def test_configure_override(self):
        default_configure(recorder=KAFKA_RECORDER)
        assert_equal(context._recorder_function, kafka.send)


class KafkaSendTestCase(TestCase):
    @setup
    def build_context(self):
        self.context = context.Context('test', 1)

    @setup
    def init_kafka(self):
        self.port = random.randint(30000, 40000)
        kafka.init('127.0.0.1', self.port)

    @setup
    def configure_kafka(self):
        context._recorder_function = kafka.send
        self.kp = MockKafkaProducer()
        kafka.KafkaProducer = self.kp

    @teardown
    def unconfigure_kafka(self):
        context._recorder_function = None

    def test(self):
        with self.context:
            self.context.set('foo', True)
            self.context.set('bar.baz', 10.0)

        data = msgpack.unpackb(self.kp.last_data)
        assert_equal(self.kp.last_topic, 'events')
        assert_equal(data['id'], 1)
        assert_equal(data['type'], 'test')
        assert_equal(utils.get_deep(data['body'], "bar.baz"), 10.0)

        kafka.close()
        assert_equal(self.kp.close_timeout, kafka.LINGER_SHUTDOWN_MSECS)


class SerializeContextTestCase(TestCase):
    @setup
    def build_context(self):
        self.context = context.Context('test', 1)

    def test_types(self):
        with self.context:
            self.context.set('decimal_value', decimal.Decimal("6.66"))
            self.context.set('date_value', datetime.date(2013, 12, 10))
            self.context.set(
                'datetime_value', datetime.datetime(2013, 12, 10, 12, 12, 12))

        context_data = kafka._serialize_context(self.context)
        data = msgpack.unpackb(context_data)
        assert_equal(data['body']['decimal_value'], "6.66")
        assert_equal(data['body']['date_value'], "2013-12-10")
        assert_equal(
            datetime.datetime.fromtimestamp(
                float(data['body']['datetime_value'])),
            datetime.datetime(2013, 12, 10, 12, 12, 12))

    def test_exception(self):
        with self.context:
            self.context.set('value', Exception('hello'))

        context_data = kafka._serialize_context(self.context)
        data = msgpack.unpackb(context_data)

        # The serialization should fail, but that just
        # means we don't have any data.
        assert_equal(data['body'], None)
