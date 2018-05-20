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

from trackall.amqp import AMQPClient, AMQPClientReader
from trackall.api import API
from trackall.config import config


def run():
    if config.get('debug', False) is True:
        log.startLogging(sys.stdout)

    # gates
    gate_config = config.get('gate')
    protocols = config.get('gate', {}).get('protocols', [])
    if gate_config and protocols:
        queues = []

        protocol_modules = \
            {protocol: importlib.import_module('trackall.protocols.{}'.format(protocol)) for protocol in protocols}
        print(' [x] Protocols loaded')

        for protocol_name, protocol_module in protocol_modules.items():
            queue = Queue()
            protocol_module.initial(gate_config.get('protocols_settings', {}).get(protocol_name), queue)
            queues.append(queue)
        print(' [x] Gate is awaiting for requests')

        amqp = AMQPClient(config['amqp']['url'], queues)
        amqp.connect()

    # database
    database_config = config.get('db_backend')
    db_engine = config.get('db_backend', {}).get('module')
    db_object = None
    if database_config and db_engine:
        db_module = importlib.import_module('trackall.db.{}'.format(db_engine))
        db_object = db_module.BackendDB(database_config)

        if database_config.get('amqp_listener') is True:
            amqp_reader = AMQPClientReader(config['amqp']['url'], db_object.insert_geo_record)
            amqp_reader.connect()
            print(' [x] AMQP Listener start')

    # api
    api_config = config.get('api')
    if api_config and db_object:
        API.run(api_config, db_object.get_geo_path)

    # Run
    reactor.run()

    return 0
