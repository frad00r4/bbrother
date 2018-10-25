# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import ujson

from twisted.web.http import OK
from twisted.web.resource import Resource
from twisted.web.server import Site, NOT_DONE_YET
from twisted.internet.endpoints import TCP4ServerEndpoint

from trackall.main import reactor


class WebApi(Resource):
    isLeaf = True

    def __init__(self, callback):
        super(WebApi, self).__init__()
        self.callback = callback

    def render_GET(self, request):
        request.setResponseCode(OK)
        request.setHeader('Access-Control-Allow-Origin', '*')
        request.setHeader('Access-Control-Allow-Methods', 'GET')
        request.setHeader('Access-Control-Allow-Headers', 'x-prototype-version,x-requested-with')
        request.setHeader('Access-Control-Max-Age', 2520)

        # TODO: Parse request
        api_request = None

        self.webapi_callback(api_request, request)
        return NOT_DONE_YET

    def _api_response(self, response, request):
        result = [
            {'lat': record[2], 'lng': record[3], 'speed': record[4], 'altitude': record[5], 'stamp': record[6]}
            for record in response
        ]
        request.write(ujson.dumps(result).encode('utf-8'))
        request.finish()

    def webapi_callback(self, api_request, request):
        d = self.callback(api_request)
        d.addCallback(self._api_response, request)


def initial(config, callback):
    site = Site(WebApi(callback))
    endpoint = TCP4ServerEndpoint(reactor, config.get('port', 80))
    endpoint.listen(site)
    print(' [x] Start Web API listen port: '+str(config.get('port', 80)))


def config_check(config):
    return True
