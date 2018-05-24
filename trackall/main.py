# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import sys
import importlib

try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty

from twisted.internet import reactor
from twisted.python import log

from trackall.amqp import AMQPClient, GatePublisherCallback, DBInsertCallback
from trackall.api import API
from trackall.config import config


def run():
    if config.get('debug', False) is True:
        log.startLogging(sys.stdout)

    protocol_modules = {}
    db_engine_module = {}

    # gates
    gate_config = config.get('gate', {})
    if gate_config:
        queues = []

        for gate_instance_name, gate_instance_config in gate_config.items():
            if not gate_instance_config.get('module'):
                continue

            if gate_instance_config['module'] not in protocol_modules.keys():
                protocol_modules[gate_instance_config['module']] = \
                    importlib.import_module('trackall.protocols.{}'.format(gate_instance_config['module']))
                print(' [x] Protocol loaded {}'.format(gate_instance_config['module']))

            queue = Queue()
            protocol_modules[gate_instance_config['module']].initial(gate_instance_config, queue)
            queues.append(queue)
            print(' [x] Gate "{}" is awaiting for requests'.format(gate_instance_name))

            gate_callback = GatePublisherCallback(gate_instance_config, queue)
            amqp_connection = AMQPClient(config['amqp']['url'], gate_callback.callback, gate_instance_name)
            amqp_connection.connect()

    # databases insert records from gate
    db_insert_backend_config = config.get('db_insert_backend', {})
    if db_insert_backend_config:

        for db_engine_instance_name, db_engine_instance_config in db_insert_backend_config.items():
            if not db_engine_instance_config.get('module'):
                continue

            if db_engine_instance_config['module'] not in db_engine_module.keys():
                db_engine_module[db_engine_instance_config['module']] = \
                    importlib.import_module('trackall.db.{}'.format(db_engine_instance_config['module']))
                print(' [x] DB engine loaded {}'.format(db_engine_instance_config['module']))

            db_object = db_engine_module[db_engine_instance_config['module']].BackendDB(db_engine_instance_config,
                                                                                        db_engine_instance_name)

            db_insert_callback = DBInsertCallback(db_engine_instance_config, db_object.insert_geo_record)
            amqp_connection = AMQPClient(config['amqp']['url'], db_insert_callback.callback, db_engine_instance_name)
            amqp_connection.connect()
            print(' [x] AMQP Gate Listener for {} start'.format(db_engine_instance_name))

    # api
    # api_config = config.get('api')
    # if api_config and db_object:
    #     API.run(api_config, db_object.get_geo_path)

    # Run
    reactor.run()

    return 0
