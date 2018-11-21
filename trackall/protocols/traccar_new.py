# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

from queue import Full
from twisted.web.http import BAD_REQUEST, OK
from twisted.web.resource import Resource
from twisted.web.server import Site
from twisted.internet.endpoints import TCP4ServerEndpoint

from trackall.objects.geo_point import GeoPoint
from trackall.objects.tracker import Tracker, IdentifyType
from trackall.main import reactor


class OSMProtocol(Resource):
    isLeaf = True
    fields = {b'id', b'lat', b'lon', b'timestamp', b'hdop', b'altitude', b'speed'}

    def __init__(self, queue):
        super(OSMProtocol, self).__init__()
        self.queue = queue

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
        try:
            self.queue.put_nowait(geo_point)
        except Full:
            print(' [!] Queue full')

        request.setResponseCode(OK)
        return b'OK'

    def render_POST(self, request):
        return self.render_GET(request)


def config_check(config):
    return True


def initial(config, queue):
    site = Site(OSMProtocol(queue))
    endpoint = TCP4ServerEndpoint(reactor, config.get('port', 8080))
    endpoint.listen(site)
