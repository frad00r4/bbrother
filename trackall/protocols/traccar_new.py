# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

try:
    from Queue import Full
except ImportError:
    from queue import Full

from twisted.web.http import BAD_REQUEST, OK
from twisted.web.resource import Resource
from twisted.web.server import Site
from twisted.internet.endpoints import TCP4ServerEndpoint

from trackall.main import reactor


class WebProtocol(Resource):
    isLeaf = True
    fields = {b'id', b'lat', b'lon', b'timestamp', b'hdop', b'altitude', b'speed'}

    def __init__(self, queue):
        super(WebProtocol, self).__init__()
        self._queue = queue

    def render_GET(self, request):
        if not request.args and isinstance(request.args, dict) and len(self.fields - set(request.args.keys())) > 0:
            request.setResponseCode(BAD_REQUEST)
            return b'Bad request'

        # Skip unneeded fields
        data = {field: request.args.get(field) for field in self.fields}
        try:
            self._queue.put_nowait(data)
        except Full:
            print(' [!] Queue full')

        request.setResponseCode(OK)
        return b'OK'

    def render_POST(self, request):
        return self.render_GET(request)


def initial(config, queue):
    site = Site(WebProtocol(queue))
    endpoint = TCP4ServerEndpoint(reactor, config.get('port', 8080))
    endpoint.listen(site)