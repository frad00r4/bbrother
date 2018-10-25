# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import ujson


METHODS = ('query', 'insert')


class DataBaseProtocol(object):
    def __init__(self, message_type, header, body=None):
        self.type = message_type
        self.header = header
        self.body = body if body else {}

    def serialize(self):
        return ujson.dumps({'message_type': self.type, 'header': self.header, 'body': self.body})

    @classmethod
    def deserialize(cls, json):
        data = ujson.loads(json)
        return cls(**data)

    @classmethod
    def geo_query(cls, request):
        header = {'method': 'query', 'type': 'geo'}
        body = {'filter': request}
        return cls('request', header, body)

    @classmethod
    def geo_response(cls, result):
        header = {'type': 'geo'}
        records = [
            {'lat': record[2], 'lng': record[3], 'speed': record[4], 'altitude': record[5], 'stamp': record[6]}
            for record in result
        ]
        body = {'geo': records}
        return cls('request', header, body)
