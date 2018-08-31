import random
import struct
import decimal
import datetime

from testify import (
    TestCase,
    setup,
    teardown,
    assert_equal)
import zmq
import msgpack

from blueox import utils
from blueox import context
from blueox.recorders import zmq as zmq_rec


class NoNetworkSendTestCase(TestCase):
    def test(self):
        """Verify that if network isn't setup, send just does nothing"""
        zmq_rec.send(context.Context('test', 1))


class NetworkSendTestCase(TestCase):
    @setup
    def build_context(self):
        self.context = context.Context('test', 1)

    @setup
    def init_network(self):
        self.port = random.randint(30000, 40000)
        zmq_rec.init("127.0.0.1", self.port)

    @setup
    def configure_network(self):
        context._recorder_function = zmq_rec.send

    @teardown
    def unconfigure_network(self):
        context._recorder_function = None

    @setup
    def build_server_socket(self):
        self.server = zmq_rec._zmq_context.socket(zmq.PULL)
        self.server.bind("tcp://127.0.0.1:%d" % self.port)

    @teardown
    def destroy_server(self):
        self.server.close(linger=0)

    @teardown
    def destory_network(self):
        zmq_rec.close()

    def test(self):
        with self.context:
            self.context.set('foo', True)
            self.context.set('bar.baz', 10.0)

        event_meta, raw_data = self.server.recv_multipart()
        zmq_rec.check_meta_version(event_meta)
        _, event_time, event_host, event_type = struct.unpack(
            zmq_rec.META_STRUCT_FMT, event_meta)
        assert_equal(event_type, 'test')

        data = msgpack.unpackb(raw_data)
        assert_equal(data['id'], 1)
        assert_equal(data['type'], 'test')
        assert_equal(utils.get_deep(data['body'], "bar.baz"), 10.0)


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

        meta_data, context_data = zmq_rec._serialize_context(self.context)
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

        meta_data, context_data = zmq_rec._serialize_context(self.context)
        data = msgpack.unpackb(context_data)

        # The serialization should fail, but that just means we don't have any
        # data.
        assert_equal(data['body'], None)
