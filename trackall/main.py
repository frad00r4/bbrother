# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import sys
import importlib

from six.moves import queue
from twisted.internet import reactor
from twisted.python import log

from trackall.amqp import AMQPClientPermanent
from trackall.amqp_callbacks import QueueCallback, ListenReplyCallback, RequestResponseCallback
from trackall.exceptions import ConfigError


class TrackAll(object):
    def __init__(self, config):
        self.protocol_modules = {}
        self.database_modules = {}
        self.api_modules = {}
        self.gate_configs = None
        self.database_configs = None
        self.api_configs = None

        self.read_config(config)

    def read_config(self, config):
        if config.get('debug', False) is True:
            log.startLogging(sys.stdout)

        # Read gate configs and load modules
        self.gate_configs = config.get('gates')
        if self.gate_configs and not isinstance(self.gate_configs, dict):
            raise ConfigError('Gates section is not dictionary')

        for gate_instance_name, gate_instance_config in self.gate_configs.items():
            for option in ('module', 'amqp_url'):
                if not gate_instance_config.get(option):
                    raise ConfigError('Gate {} has not got option "{}"'.format(gate_instance_name, option))

            if gate_instance_config['module'] not in self.protocol_modules.keys():
                self.protocol_modules[gate_instance_config['module']] = \
                    importlib.import_module('trackall.protocols.{}'.format(gate_instance_config['module']))
                print(' [x] Protocol loaded {}'.format(gate_instance_config['module']))

            if not self.protocol_modules[gate_instance_config['module']].config_check(gate_instance_config):
                raise ConfigError('Gate %s: invalid config' % gate_instance_name)

        # Read databases configs and load modules
        self.database_configs = config.get('databases')
        if self.database_configs and not isinstance(self.database_configs, dict):
            raise ConfigError('Databases section is not dictionary')

        for database_instance_name, database_instance_config in self.database_configs.items():
            for option in ('module', 'amqp_url', 'listen_queue'):
                if not database_instance_config.get(option):
                    raise ConfigError('Database {} has not got option "{}"'.format(database_instance_name, option))

            if database_instance_config['module'] not in self.database_modules.keys():
                self.database_modules[database_instance_config['module']] = \
                    importlib.import_module('trackall.db.{}'.format(database_instance_config['module']))
                print(' [x] Database module loaded {}'.format(database_instance_config['module']))

            if not self.database_modules[database_instance_config['module']].config_check(database_instance_config):
                raise ConfigError('Database module %s: invalid config' % database_instance_name)

        # Read API configs and load modules
        self.api_configs = config.get('api')
        if self.api_configs and not isinstance(self.api_configs, dict):
            raise ConfigError('API section is not dictionary')

        for api_instance_name, api_instance_config in self.api_configs.items():
            for option in ('module', 'amqp_url'):
                if not api_instance_config.get(option):
                    raise ConfigError('API {} has not got option "{}"'.format(api_instance_name, option))

            if api_instance_config['module'] not in self.api_modules.keys():
                self.api_modules[api_instance_config['module']] = \
                    importlib.import_module('trackall.api.{}'.format(api_instance_config['module']))
                print(' [x] API module loaded {}'.format(api_instance_config['module']))

            if not self.api_modules[api_instance_config['module']].config_check(api_instance_config):
                raise ConfigError('Database module %s: invalid config' % api_instance_name)

    def init_gates(self):
        publish_queues = []
        for gate_instance_name, gate_instance_config in self.gate_configs.items():
            publish_queue = queue.Queue()
            self.protocol_modules[gate_instance_config['module']].initial(gate_instance_config, publish_queue)
            publish_queues.append(publish_queue)
            print(' [x] Gate "{}" is awaiting for requests'.format(gate_instance_name))
            amqp_callback = QueueCallback(gate_instance_config, publish_queue)
            amqp_connection = AMQPClientPermanent(
                gate_instance_config['amqp_url'],
                amqp_callback.callback,
                gate_instance_name
            )
            amqp_connection.run()

    def init_database(self):
        for database_instance_name, database_instance_config in self.database_configs.items():
            db_callback = self.database_modules[database_instance_config['module']].initial(database_instance_config)
            print(' [x] Database connection "{}" is running'.format(database_instance_name))
            amqp_callback = ListenReplyCallback(database_instance_config['listen_queue'], db_callback)
            amqp_connection = AMQPClientPermanent(
                database_instance_config['amqp_url'],
                amqp_callback.callback,
                database_instance_name
            )
            amqp_connection.run()

    def init_api(self):
        for api_instance_name, api_instance_config in self.api_configs.items():
            callback = RequestResponseCallback(api_instance_name, api_instance_config)
            self.api_modules[api_instance_config['module']].initial(api_instance_config,
                                                                    callback.api_callback)
            print(' [x] API connection "{}" is running'.format(api_instance_name))

    def run(self):
        if self.gate_configs:
            self.init_gates()
        if self.database_configs:
            self.init_database()
        if self.api_configs:
            self.init_api()

        reactor.run()

        return 0
