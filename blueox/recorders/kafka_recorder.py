# -*- coding: utf-8 -*-
"""
blueox.kafka
~~~~~~~~

This module provides the interface into Kafka

:copyright: (c) 2018 by Aaron Biller??
:license: ISC, see LICENSE for more details.

"""
import atexit
import logging
import msgpack

from kafka import KafkaProducer

from .. import ports
from .. import utils

log = logging.getLogger(__name__)

# If we have pending outgoing messages, this is how long we'll wait after
# being told to exit.
LINGER_SHUTDOWN_MSECS = 2000

# Producer can be shared between threads
_kafka_producer = None


def init(host=None):
    """Initialize the global kafka producer

    Supports a host arg with an overriding kafka host string
    in the format 'hostname:port'
    """
    global _kafka_producer

    host = ports.default_kafka_host(host)

    _kafka_producer = KafkaProducer(bootstrap_servers=host)


def _serialize_context(context):
    context_dict = context.to_dict()
    for key in ('host', 'type'):
        if len(context_dict.get(key, "")) > 64:
            raise ValueError("Value too long: %r" % key)

    context_dict = {
        k: v.encode('utf-8') if isinstance(v, unicode)
        else v for k, v in context_dict.items()
    }

    try:
        context_data = msgpack.packb(context_dict)
    except TypeError:
        try:
            # If we fail to serialize our context, we can try again with an
            # enhanced packer (it's slower though)
            context_data = msgpack.packb(context_dict,
                                         default=utils.msgpack_encode_default)
        except TypeError:
            log.exception("Serialization failure (not fatal, dropping data)")

            # One last try after dropping the body
            context_dict['body'] = None
            context_data = msgpack.packb(context_dict)

    return context_data


def send(context):
    global _kafka_producer

    try:
        context_data = _serialize_context(context)
    except Exception:
        log.exception("Failed to serialize context")
        return

    if _kafka_producer:
        try:
            log.debug("Sending msg")
            _kafka_producer.send('events', context_data)
        except Exception:
            log.exception("Failed during publish to kafka.")
    else:
        log.info("Skipping sending event %s", context.name)


def close():
    global _kafka_producer

    if _kafka_producer:
        _kafka_producer.flush()
        _kafka_producer.close(timeout=LINGER_SHUTDOWN_MSECS)
    _kafka_producer = None


atexit.register(close)
