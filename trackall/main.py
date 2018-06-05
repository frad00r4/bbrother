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

from trackall.amqp import AMQPClient, GatePublisherCallback, DBInsertCallback, DBReadGeoDataCallback
from trackall.config import config


def run():
    if config.get('debug', False) is True:
        log.startLogging(sys.stdout)

    protocol_modules = {}
    db_engine_modules = {}
    external_api_modules = {}

    # gates
    gate_config = config.get('gate')
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
    db_write_backend_config = config.get('db_write_backend')
    if db_write_backend_config:

        for db_engine_instance_name, db_engine_instance_config in db_write_backend_config.items():
            if not db_engine_instance_config.get('module'):
                continue

            if db_engine_instance_config['module'] not in db_engine_modules.keys():
                db_engine_modules[db_engine_instance_config['module']] = \
                    importlib.import_module('trackall.db.{}'.format(db_engine_instance_config['module']))
                print(' [x] DB engine loaded {}'.format(db_engine_instance_config['module']))

            db_object = db_engine_modules[db_engine_instance_config['module']].BackendDB(db_engine_instance_config,
                                                                                        db_engine_instance_name)

            db_insert_callback = DBInsertCallback(db_engine_instance_config, db_object.insert_geo_record)
            amqp_connection = AMQPClient(config['amqp']['url'], db_insert_callback.callback, db_engine_instance_name)
            amqp_connection.connect()
            print(' [x] AMQP Gate Listener for {} start'.format(db_engine_instance_name))

    # databases read records for api
    db_read_backend_config = config.get('db_read_backend')
    if db_read_backend_config:
        for db_engine_instance_name, db_engine_instance_config in db_read_backend_config.items():
            if not db_engine_instance_config.get('module'):
                continue

            if db_engine_instance_config['module'] not in db_engine_modules.keys():
                db_engine_modules[db_engine_instance_config['module']] = \
                    importlib.import_module('trackall.db.{}'.format(db_engine_instance_config['module']))
                print(' [x] DB engine loaded {}'.format(db_engine_instance_config['module']))

            db_object = db_engine_modules[db_engine_instance_config['module']].BackendDB(db_engine_instance_config,
                                                                                        db_engine_instance_name)

            for i in range(int(db_engine_instance_config.get('listeners', 1))):
                db_read_callback = DBReadGeoDataCallback(db_engine_instance_config, db_object.get_geo_path)
                amqp_connection = AMQPClient(config['amqp']['url'], db_read_callback.callback, db_engine_instance_name)
                amqp_connection.connect()
                print(' [x] AMQP DB Read Listener {} for {} start'.format(i, db_engine_instance_name))

    # api
    external_api_config = config.get('external_api')
    if external_api_config:
        for external_api_instance_name, external_api_instance_config in external_api_config.items():
            if not external_api_instance_config.get('module'):
                continue

            if external_api_instance_config['module'] not in external_api_modules.keys():
                external_api_modules[external_api_instance_config['module']] = \
                    importlib.import_module('trackall.api.{}'.format(external_api_instance_config['module']))
                print(' [x] External API module {} loaded'.format(external_api_instance_config['module']))

    # Run
    reactor.run()

    return 0
