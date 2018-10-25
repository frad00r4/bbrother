# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

from pymysql.err import OperationalError
from twisted.enterprise.adbapi import ConnectionPool
from twisted.internet.defer import inlineCallbacks, returnValue

from trackall.main import reactor


class Database(object):
    def __init__(self, config):
        self.db_engine = config.get('engine', 'MySQLdb')
        self.database = config.get('database', 'tracker')
        self.user = config.get('user', 'root')
        self.db_pool = None
        self.connect()

    def connect(self):
        self.db_pool = ConnectionPool(self.db_engine, database=self.database, user=self.user)

    @inlineCallbacks
    def callback(self, message):
        try:
            result = yield self.db_pool.runQuery('select * from test ORDER BY stamp ASC;')
            # yield self.db_pool.runQuery(
            #     'INSERT INTO test (user_id, lat, lon, speed, altitude, stamp)'
            #     ' VALUES ({id}, {lat}, {lon}, {speed}, {altitude}, FROM_UNIXTIME({timestamp}));'.format(**message)
            # )
            # result = None
        except OperationalError as e:
            print(' [!] Connection failed: {}'.format(e))
            self.db_pool.close()
            self.connect()
            result = yield reactor.callLater(1, self.callback, message)

        returnValue(result)


def config_check(config):
    return True


def initial(config):
    db = Database(config)
    db.connect()
    return db.callback
