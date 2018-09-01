import datetime
import decimal
import json
import random

from testify import (
    TestCase,
    setup,
    teardown,
    assert_equal,
    assert_raises)

from pycernan.avro.serde import serialize
from pycernan.avro.exceptions import DatumTypeException

from blueox import default_configure, PYCERNAN_RECORDER
from blueox import utils
from blueox import context
from blueox.recorders import pycernan as pycernan_rec
from blueox.recorders import zmq


class MockPycernanClient(object):
    last_schema = None
    last_batch = None
    last_sync = None

    def __call__(self, host=None, port=None):
        self.host = host
        self.port = port
        return self

    def publish(self, schema, batch, sync=None):
        self.last_schema = schema
        self.last_batch = batch
        self.last_sync = sync

    def close(self):
        pass


class CantSerializeMe(object):
    def __repr__(self):
        return chr(167)


class PycernanOverrideTestCase(TestCase):
    def test_configure_no_override(self):
        default_configure()
        assert_equal(context._recorder_function, zmq.send)

    def test_configure_override(self):
        pycernan_rec.Client = MockPycernanClient()
        default_configure(recorder=PYCERNAN_RECORDER)
        assert_equal(context._recorder_function, pycernan_rec.send)


class PycernanSendTestCase(TestCase):
    @setup
    def build_context(self):
        self.context = context.Context('test', 1)

    @setup
    def init_pycernan(self):
        self.port = random.randint(30000, 40000)
        self.client = MockPycernanClient()
        pycernan_rec.Client = self.client
        pycernan_rec.init('127.0.0.1', self.port)

    @setup
    def configure_pycernan(self):
        context._recorder_function = pycernan_rec.send

    @teardown
    def unconfigure_pycernan(self):
        context._recorder_function = None

    @teardown
    def destroy_recorder(self):
        pycernan_rec.close()

    def test(self):
        with self.context:
            self.context.set('foo', True)
            self.context.set('bar.baz', 10.0)

        data = self.client.last_batch[0]
        data['body'] = json.loads(data['body'])
        assert_equal(self.client.last_schema, pycernan_rec.BLUEOX_AVRO_RECORD)
        assert_equal(self.client.last_sync, False)
        assert_equal(data['id'], '1')
        assert_equal(data['type'], 'test')
        assert_equal(utils.get_deep(data['body'], 'bar.baz'), 10.0)

        assert_equal(self.client.host, '127.0.0.1')
        assert_equal(self.client.port, self.port)


class SerializeContextTestCase(TestCase):
    @setup
    def build_context(self):
        self.context = context.Context('test', 1)

    def test_types(self):
        with self.context:
            self.context.set('decimal_value', decimal.Decimal('6.66'))
            self.context.set('date_value', datetime.date(2013, 12, 10))
            self.context.set(
                'datetime_value', datetime.datetime(2013, 12, 10, 12, 12, 12))

        data = pycernan_rec._serialize_context(self.context)
        data['body'] = json.loads(data['body'])
        assert_equal(data['body']['decimal_value'], 6.66)
        assert_equal(data['body']['date_value'], '2013-12-10')
        assert_equal(
            datetime.datetime.strptime(
                data['body']['datetime_value'], '%Y-%m-%dT%H:%M:%S'),
            datetime.datetime(2013, 12, 10, 12, 12, 12))

    def test_exception(self):
        with self.context:
            self.context.set('value', CantSerializeMe())

        data = pycernan_rec._serialize_context(self.context)

        # The serialization should fail, but that just
        # means we don't have any data.
        assert_equal(data['body'], None)


class EncodeAvroTestCase(TestCase):
    @setup
    def build_context(self):
        self.context = context.Context('test', 1)

    def test_success(self):
        with self.context:
            self.context.set('foo', True)
            self.context.set('bar.baz', 10.0)

        data = pycernan_rec._serialize_context(self.context)
        serialize(pycernan_rec.BLUEOX_AVRO_RECORD, [data])

    def test_failure(self):
        with self.context:
            self.context.set('foo', True)
            self.context.set('bar.baz', 10.0)
            self.context.set('decimal_value', decimal.Decimal('6.66'))
            self.context.set('date_value', datetime.date(2013, 12, 10))
            self.context.set(
                'datetime_value', datetime.datetime(2013, 12, 10, 12, 12, 12))

        data = pycernan_rec._serialize_context(self.context)
        data['host'] = None
        with assert_raises(DatumTypeException):
            serialize(pycernan_rec.BLUEOX_AVRO_RECORD, [data])

    def test_none_body(self):
        with self.context:
            self.context.set('bad_char', CantSerializeMe())

        data = pycernan_rec._serialize_context(self.context)
        assert_equal(data['body'], None)
        serialize(pycernan_rec.BLUEOX_AVRO_RECORD, [data])
