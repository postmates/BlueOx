# -*- coding: utf-8 -*-
"""
blueox
~~~~~~~~

:copyright: (c) 2012 by Rhett Garber
:license: ISC, see LICENSE for more details.

"""

__title__ = 'blueox'
__version__ = '0.11.6.3'
__author__ = 'Rhett Garber'
__author_email__ = 'rhettg@gmail.com'
__license__ = 'ISC'
__copyright__ = 'Copyright 2015 Rhett Garber'
__description__ = 'A library for python-based application logging and data collection'
__url__ = 'https://github.com/rhettg/BlueOx'

import logging

from . import utils
from . import ports
from .context import (
    Context, set, append, add, context_wrap, current_context, find_context,
    clear_contexts)
from . import context as _context_mod
from .errors import Error
from .logger import LogHandler
from .timer import timeit
from .recorders import kafka, zmq

log = logging.getLogger(__name__)

ZMQ_RECORDER = 'zmq'
KAFKA_RECORDER = 'kafka'
RECORDERS = {
    ZMQ_RECORDER: zmq,
    KAFKA_RECORDER: kafka,
}
DEFAULT_RECORDER = ZMQ_RECORDER


def configure(host, port, recorder=None):
    """Initialize blueox

    This instructs the blueox system where to send its logging data.
    If blueox is not configured, log data will be silently dropped.

    Currently we support logging through the network (and the configured host
    and port) to a blueoxd instances, or to the specified recorder function.
    """
    if callable(recorder):
        _context_mod._recorder_function = recorder

    else:
        _rec = RECORDERS.get(recorder, None)

        if _rec is not None:
            _rec.init(host, port)
            _context_mod._recorder_function = _rec.send
        else:
            log.info("Empty blueox configuration")
            _context_mod._recorder_function = None


def default_configure(host=None, recorder=DEFAULT_RECORDER):
    """Configure BlueOx based on defaults

    Accepts a connection string override in the form `localhost:3514`. Respects
    environment variable BLUEOX_HOST
    """
    _rec = RECORDERS.get(recorder, None)
    if _rec is None:
        _rec = RECORDERS.get(DEFAULT_RECORDER)

    host = _rec.default_host(host)
    hostname, port = host.split(':')

    try:
        int_port = int(port)
    except ValueError:
        raise Error("Invalid value for port")

    configure(hostname, int_port, recorder=recorder)


def shutdown():
    zmq.close()
    kafka.close()
