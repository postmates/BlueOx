# -*- coding: utf-8 -*-
"""
blueox.recorders.kafka
~~~~~~~~

This module provides the interface into Kafka

:copyright: (c) 2018 by Aaron Biller??
:license: ISC, see LICENSE for more details.

"""
from __future__ import absolute_import

import atexit
import logging
import msgpack
import threading

from kafka import KafkaProducer

from blueox import ports
from blueox import utils

log = logging.getLogger(__name__)

# If we have pending outgoing messages, this is how long we'll wait after
# being told to exit.
LINGER_SHUTDOWN_MSECS = 2000


def default_host(host=None):
    """Build a default host string for the kafka producer
    """
    return ports.default_kafka_host(host)


threadLocal = threading.local()

# Context can be shared between threads
_kafka_hosts = None


def init(host, port):
    global _kafka_hosts

    _kafka_hosts = '{}:{}'.format(host, port)


def _thread_connect():
    if _kafka_hosts and not getattr(threadLocal, 'kp', None):
        threadLocal.kp = KafkaProducer(bootstrap_servers=_kafka_hosts)


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
    _thread_connect()

    try:
        context_data = _serialize_context(context)
    except Exception:
        log.exception("Failed to serialize context")
        return

    if _kafka_hosts and threadLocal.kp is not None:
        try:
            log.debug("Sending msg")
            threadLocal.kp.send('events', context_data)
        except Exception:
            log.exception("Failed during publish to kafka.")
    else:
        log.info("Skipping sending event %s", context.name)


def close():
    if getattr(threadLocal, 'kp', None):
        threadLocal.kp.flush()
        threadLocal.kp.close(timeout=LINGER_SHUTDOWN_MSECS)
        threadLocal.kp = None


atexit.register(close)
