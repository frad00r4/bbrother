# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import sys
import importlib

try:
    from Queue import Queue
except ImportError:
    from queue import Queue

from twisted.internet import reactor
from twisted.python import log

from trackall.amqp import AMQPClient
from trackall.config import config


def run():
    if config.get('gate', {}).get('debug') is True:
        log.startLogging(sys.stdout)

    protocols = config.get('gate', {}).get('protocols', [])
    queues = []

    protocol_modules = \
        {protocol: importlib.import_module('trackall.protocols.{}'.format(protocol)) for protocol in protocols}
    print(' [x] Protocols loaded')

    for protocol_name, protocol_module in protocol_modules.items():
        queue = Queue()
        protocol_module.initial(config.get('gate', {}).get('protocols_settings', {}).get(protocol_name), queue)
        queues.append(queue)
    print(' [x] Gate is awaiting for requests')

    amqp = AMQPClient(config['amqp']['url'], queues)
    amqp.connect()

    reactor.run()

    return 0
