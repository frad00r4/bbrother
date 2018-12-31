# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import ujson
import txredisapi as redis

from twisted.internet.defer import inlineCallbacks


class RedisCache(object):
    def __init__(self, address: str, port: int, name: str = '', database: str = '0', password: str = None):
        self.parameters = {
            'host': address,
            'port': port,
            'dbid': database,
            'password': password
        }
        self.connection = None
        self.name = name

    def make_connection(self, connection: redis.ConnectionHandler):
        if not connection:
            return None
        print(' [x] Connected to cache {}'.format(self.name))
        self.connection = connection

    def run(self):
        d = redis.Connection(**self.parameters)
        d.addCallback(self.make_connection)
        return d

    @inlineCallbacks
    def get(self, key: str):
        result_raw = yield self.connection.get(key)
        result = ujson.loads(result_raw) if result_raw else None
        return result

    @inlineCallbacks
    def set(self, key: str, data: dict):
        data_raw = ujson.dumps(data)
        yield self.connection.set(key, data_raw)
        return True
