# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

from typing import Dict
from pymysql.err import OperationalError
from twisted.enterprise.adbapi import ConnectionPool
from twisted.internet.defer import inlineCallbacks

from bbrother.objects.geo_point import GeoPoint
from bbrother.objects.user import User
from bbrother.objects.db_proto import DataBasePackage, Method, Target, DataBaseResponse
from bbrother.main import reactor


class Database(object):
    def __init__(self, config: Dict) -> None:
        self.db_engine = config.get('engine', 'MySQLdb')
        self.database = config.get('database', 'tracker')
        self.user = config.get('user', 'root')
        self.db_pool = None
        self.connect()

    def connect(self):
        self.db_pool = ConnectionPool(self.db_engine, database=self.database, user=self.user)

    @inlineCallbacks
    def callback(self, message: str) -> DataBaseResponse:
        db_package = DataBasePackage.deserialize(message)
        result = None
        try:
            if db_package.method == Method.insert:
                if isinstance(db_package.target, GeoPoint):
                    params = {
                        'id': db_package.target.tracker.id_str,
                        'lat': db_package.target.latitude,
                        'lon': db_package.target.longitude,
                        'speed': db_package.target.speed,
                        'altitude': db_package.target.altitude,
                        'timestamp': db_package.target.timestamp
                    }
                    # TODO: Fix hardcode tracker id
                    yield self.db_pool.runQuery('''
INSERT INTO geodata 
    (tracker_id, lat, lon, speed, altitude, stamp)
VALUES 
    (1, {lat}, {lon}, {speed}, {altitude}, FROM_UNIXTIME({timestamp}));
'''.format(**params))

            if db_package.method == Method.select:
                query = ''
                selectors = []

                if db_package.selector.target == Target.geo:
                    query = 'SELECT * FROM geodata'
                elif db_package.selector.target == Target.user:
                    query = 'SELECT * FROM users'
                    if db_package.selector.selector.get('login'):
                        selectors.append('login = "%s"' % db_package.selector.selector.get('login'))
                    if db_package.selector.selector.get('password_hash'):
                        selectors.append('password_hash = "%s"' % db_package.selector.selector.get('password_hash'))
                    if db_package.selector.selector.get('user_id'):
                        selectors.append('id = %s' % db_package.selector.selector.get('user_id'))

                if selectors:
                    query += ' WHERE {}'.format(' AND '.join(selectors))
                if db_package.selector.offset:
                    query += ' OFFSET %d' % db_package.selector.offset
                if db_package.selector.limit:
                    query += ' LIMIT %d' % db_package.selector.limit

                rows = yield self.db_pool.runQuery(query)

                if db_package.selector.target == Target.geo:
                    response = [GeoPoint(
                        latitude=row[2],
                        longitude=row[3],
                        altitude=row[5],
                        timestamp=row[6],
                        speed=row[4]
                    ) for row in rows]
                elif db_package.selector.target == Target.user:
                    response = [User(
                        user_id=row[0],
                        login=row[1],
                        password_hash=row[2],
                        stamp=row[3]
                    ) for row in rows]
                else:
                    response = []

                result = DataBaseResponse(response)

        except OperationalError as e:
            print(' [!] Connection failed: {}'.format(e))
            self.db_pool.close()
            self.connect()
            result = yield reactor.callLater(1, self.callback, message)

        return result


def config_check(config: Dict) -> bool:
    # TODO: Add checker
    return True


def initial(config: Dict):
    db = Database(config)
    db.connect()
    return db.callback
