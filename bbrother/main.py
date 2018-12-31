# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import sys
import importlib
import copy

from twisted.internet import reactor
from twisted.python import log

from bbrother.amqp import AMQPClientPermanent, AMQPClientPermanentCallback
from bbrother.amqp_callbacks import ListenReplyCallback, RequestResponseCallback
from bbrother.exceptions import ConfigError
from bbrother.redis import RedisCache


class TrackAll(object):
    def __init__(self, config):
        self.protocol_modules = {}
        self.database_modules = {}
        self.api_modules = {}
        self.gate_configs = None
        self.database_configs = None
        self.api_configs = None
        self.base_config = None

        self.read_config(config)

    def read_config(self, config):
        if config.get('debug', False) is True:
            log.startLogging(sys.stdout)

        # Make base config data
        base_config = {
            item: config.get(item)
            for item in ('amqp_url', 'redis_server', 'redis_port', 'redis_db')
            if config.get(item) is not None
        }

        loader_data_list = [
            ['Gate', {'module', 'amqp_url'}, 'gate_configs', 'gates', 'protocol_modules'],
            ['Database', {'module', 'amqp_url', 'listen_queue'}, 'database_configs', 'databases', 'database_modules'],
            ['API', {'module', 'amqp_url'}, 'api_configs', 'api', 'api_modules']
        ]

        for mod_type_name, base_options, configs_attr, configs_section, modules_attr in loader_data_list:
            setattr(self, configs_attr, {})
            configs = config.get(configs_section)
            if configs and not isinstance(configs, dict):
                raise ConfigError('{} section is not dictionary'.format(mod_type_name))

            for instance_name, instance_config in configs.items():
                instance_config_full = copy.copy(base_config)
                instance_config_full.update(instance_config)

                options = list(base_options - set(instance_config_full.keys()))
                for option in options:
                    if not config.get(option):
                        raise ConfigError('{} {} has not got option "{}"'.format(mod_type_name, instance_name, option))

                if instance_config['module'] not in getattr(self, modules_attr, {}).keys():
                    getattr(self, modules_attr, {})[instance_config_full['module']] = \
                        importlib.import_module('bbrother.{}.{}'.format(configs_section,
                                                                        instance_config_full['module']))
                    print(' [x] {} loaded {}'.format(mod_type_name, instance_config_full['module']))

                if not getattr(self, modules_attr, {})[instance_config_full['module']].config_check(instance_config_full):
                    raise ConfigError('{} {}: invalid config'.format(mod_type_name, instance_name))

                getattr(self, configs_attr, {})[instance_name] = instance_config_full

    def init_gates(self):
        for gate_instance_name, gate_instance_config in self.gate_configs.items():
            # TODO: fork system
            amqp_callback = RequestResponseCallback(gate_instance_name, gate_instance_config)
            amqp_connection = AMQPClientPermanentCallback(
                gate_instance_config['amqp_url'],
                amqp_callback.callback,
                gate_instance_name
            )
            amqp_connection.run()

            self.protocol_modules[gate_instance_config['module']].initial(gate_instance_config,
                                                                          amqp_connection.run_callback)
            print(' [x] Gate "{}" is awaiting for requests'.format(gate_instance_name))

    def init_database(self):
        for database_instance_name, database_instance_config in self.database_configs.items():
            # TODO: fork system
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
            # TODO: fork system
            amqp_callback = RequestResponseCallback(api_instance_name, api_instance_config)
            amqp_connection = AMQPClientPermanentCallback(
                api_instance_config['amqp_url'],
                amqp_callback.callback,
                api_instance_name
            )
            amqp_connection.run()

            cache_connection = RedisCache(
                api_instance_config['redis_server'],
                api_instance_config['redis_port'],
                api_instance_name,
                api_instance_config.get('redis_db', '0'),
                api_instance_config.get('redis_password')
            )
            cache_connection.run()

            self.api_modules[api_instance_config['module']].initial(api_instance_config,
                                                                    amqp_connection.run_callback,
                                                                    cache_connection)
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
