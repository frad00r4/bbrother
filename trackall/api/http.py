# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import ujson

from twisted.web.http import OK
from twisted.web.resource import Resource
from twisted.web.server import Site, NOT_DONE_YET
from twisted.internet.endpoints import TCP4ServerEndpoint

from trackall.main import reactor


class API(Resource):
    isLeaf = True

    def __init__(self, db_callback):
        super(API, self).__init__()
        self._db_callback = db_callback

    def render_GET(self, request):
        request.setResponseCode(OK)
        request.setHeader('Access-Control-Allow-Origin', '*')
        request.setHeader('Access-Control-Allow-Methods', 'GET')
        request.setHeader('Access-Control-Allow-Headers', 'x-prototype-version,x-requested-with')
        request.setHeader('Access-Control-Max-Age', 2520)
        d = self._db_callback()
        d.addCallback(self._delayedRender, request)
        return NOT_DONE_YET

    def _delayedRender(self, response, request):
        result = [{'lat': record[2], 'lng': record[3], 'speed': record[4], 'altitude': record[5], 'stamp': record[6]}
                  for record in response]
        request.write(ujson.dumps(result).encode('utf-8'))
        request.finish()

    @staticmethod
    def run(config, db_callback):
        site = Site(API(db_callback))
        endpoint = TCP4ServerEndpoint(reactor, config.get('port', 80))
        endpoint.listen(site)
