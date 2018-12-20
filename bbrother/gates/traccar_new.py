# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

from typing import Dict
from twisted.web.http import BAD_REQUEST
from twisted.web.resource import Resource
from twisted.web.server import Site, NOT_DONE_YET
from twisted.internet.endpoints import TCP4ServerEndpoint

from bbrother.objects.db_proto import DataBasePackage, Method
from bbrother.objects.geo_point import GeoPoint
from bbrother.objects.tracker import Tracker, IdentifyType
from bbrother.main import reactor


class OSMProtocol(Resource):
    isLeaf = True
    fields = {b'id', b'lat', b'lon', b'timestamp', b'hdop', b'altitude', b'speed'}

    def __init__(self, callback):
        super(OSMProtocol, self).__init__()
        self.callback = callback

    def render_GET(self, request):
        if not request.args and isinstance(request.args, dict) and len(self.fields - set(request.args.keys())) > 0:
            request.setResponseCode(BAD_REQUEST)
            return b'Bad request'

        tracker = Tracker(IdentifyType.IMEI, request.args[b'id'][0])
        geo_point = GeoPoint(
            latitude=float(request.args[b'lat'][0]),
            longitude=float(request.args[b'lon'][0]),
            altitude=float(request.args[b'altitude'][0]),
            timestamp=int(request.args[b'timestamp'][0]),
            speed=float(request.args[b'speed'][0]),
            tracker=tracker
        )
        db_package = DataBasePackage(Method.insert, geo_point)
        d = self.callback(db_package)
        d.addCallback(self.proto_response, request)
        return NOT_DONE_YET

    def render_POST(self, request):
        return self.render_GET(request)

    def proto_response(self, response: str, request):
        request.write(b'OK')
        request.finish()


def config_check(config: Dict) -> bool:
    # TODO: Add checker
    return True


def initial(config: Dict, callback):
    site = Site(OSMProtocol(callback))
    endpoint = TCP4ServerEndpoint(reactor, config.get('port', 8080))
    endpoint.listen(site)
