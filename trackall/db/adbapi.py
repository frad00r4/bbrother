# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

from pymysql.err import OperationalError
from twisted.enterprise.adbapi import ConnectionPool
from twisted.internet.defer import inlineCallbacks, returnValue

from trackall.main import reactor


class BackendDB(object):
    def __init__(self, config, name):
        self.db_engine = config.get('engine', 'MySQLdb')
        self.database = config.get('database', 'tracker')
        self.user = config.get('user', 'root')
        self.name = name
        self.db_pool = None
        self.connect()

    def connect(self):
        self.db_pool = ConnectionPool(self.db_engine, database=self.database, user=self.user)

    @inlineCallbacks
    def insert_geo_record(self, record):
        try:
            yield self.db_pool.runQuery(
                'INSERT INTO test (user_id, lat, lon, speed, altitude, stamp)'
                ' VALUES ({id}, {lat}, {lon}, {speed}, {altitude}, FROM_UNIXTIME({timestamp}));'.format(**record)
            )
        except OperationalError as e:
            print(' [!] Connection failed: {}'.format(e))
            self.db_pool.close()
            self.connect()
            yield reactor.callLater(2, self.insert_geo_record, record)

        returnValue(None)

    def get_geo_path(self):
        try:
            return self.db_pool.runQuery('select * from test ORDER BY stamp ASC;')
        except OperationalError:
            self.db_pool.close()
            self.connect()
            return self.get_geo_path()
