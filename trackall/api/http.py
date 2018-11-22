# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import ujson

from twisted.web.http import OK
from twisted.web.resource import Resource
from twisted.web.server import Site, NOT_DONE_YET
from twisted.internet.endpoints import TCP4ServerEndpoint

from trackall.objects.db_proto import DataBasePackage, Method, Target, Selector, DataBaseResponse
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
        request.setHeader('Content-Type', 'application/json')

        # TODO: Parse request
        selector = Selector(Target.geo, {})
        api_request = DataBasePackage(Method.select, selector=selector)

        d = self.callback(api_request)
        d.addCallback(self.proto_response, request)

        return NOT_DONE_YET

    def proto_response(self, response: str, request):
        db_response = DataBaseResponse.deserialize(response)
        result = [{'lat': obj.latitude, 'lng': obj.longitude, 'speed': obj.speed,
                   'altitude': obj.altitude, 'stamp': obj.timestamp}
                  for obj in db_response.objects]
        request.write(ujson.dumps(result).encode('utf-8'))
        request.finish()


def initial(config, callback):
    site = Site(WebApi(callback))
    endpoint = TCP4ServerEndpoint(reactor, config.get('port', 80))
    endpoint.listen(site)
    print(' [x] Start Web API listen port: '+str(config.get('port', 80)))


def config_check(config):
    # TODO: Add checker
    return True
