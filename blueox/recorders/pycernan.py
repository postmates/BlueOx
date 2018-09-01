# -*- coding: utf-8 -*-
"""
blueox.recorders.pycernan
~~~~~~~~

This module provides the interface into pycernan

:copyright: (c) 2018 by Aaron Biller??
:license: ISC, see LICENSE for more details.

"""
from __future__ import absolute_import

import atexit
import datetime
import decimal
import json
import logging
import os
import threading

from pycernan.avro import Client

from blueox import ports

log = logging.getLogger(__name__)

_uname = os.uname()[1]

# Global blueox avro schema definition
BLUEOX_AVRO_RECORD = {
    "doc": "A BlueOx event",
    "name": "blueox_event",
    "namespace": "blueox.{}".format(_uname),
    "type": "record",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "type", "type": "string"},
        {"name": "host", "type": "string"},
        {"name": "pid", "type": "long"},
        {"name": "start", "type": "double"},
        {"name": "end", "type": "double"},
        {"name": "body", "type": ["null", "string"], "default": "null"}
    ]
}


def default_host(host=None):
    """Build a default host string for pycernan
    """
    return ports.default_pycernan_host(host)


def _serializer(obj):
    """Serialize native python objects
    """
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    try:
        obj = str(obj)
    except Exception:
        raise TypeError(repr(obj) + ' is not JSON serializable')
    return obj


threadLocal = threading.local()

# Context can be shared between threads
_client = None


def init(host, port):
    global _client

    _client = Client(host=host, port=port)


def _thread_connect():
    if _client and not getattr(threadLocal, 'client', None):
        threadLocal.client = _client


def _serialize_context(context):
    context_dict = context.to_dict()
    for key in ('host', 'type'):
        if len(context_dict.get(key, '')) > 64:
            raise ValueError('Value too long: %r' % key)

    context_dict['id'] = str(context_dict['id'])

    body = context_dict.get('body', None)
    if body is not None:
        try:
            context_dict['body'] = json.dumps(body, default=_serializer)
        except (TypeError, ValueError):
            try:
                context_dict['body'] = unicode(body)
            except Exception:
                log.exception(
                    'Serialization failure (not fatal, dropping data)')
                context_dict['body'] = None

    context_dict = {
        k: v.encode('utf-8') if isinstance(v, unicode)
        else v for k, v in context_dict.items()
    }

    return context_dict


def send(context):
    _thread_connect()

    try:
        context_data = [_serialize_context(context)]
    except Exception:
        log.exception('Failed to serialize context')
        return

    if _client and threadLocal.client is not None:
        try:
            log.debug('Sending msg')
            threadLocal.client.publish(
                BLUEOX_AVRO_RECORD, context_data, sync=False)
        except Exception:
            log.exception('Failed during publish to pycernan.')
    else:
        log.info('Skipping sending event %s', context.name)


def close():
    if getattr(threadLocal, 'client', None):
        threadLocal.client.close()
        threadLocal.client = None


atexit.register(close)
