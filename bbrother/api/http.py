# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import abc
import ujson
import hashlib

from twisted.python.url import URL
from twisted.web.http import OK, BAD_REQUEST, NOT_ALLOWED, UNAUTHORIZED, Request
from twisted.web.resource import Resource
from twisted.web.server import Site, NOT_DONE_YET
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet.defer import inlineCallbacks
from typing import Callable, Dict

from bbrother.objects.db_proto import DataBasePackage, Method, Target, Selector, DataBaseResponse
from bbrother.redis import RedisCache
from bbrother.main import reactor


class SessionResource(Resource):
    __metaclass__ = abc.ABCMeta

    def __init__(self, config: Dict, callback: Callable, cache_interface: RedisCache):
        super(SessionResource, self).__init__()
        self.config = config
        self.db_interface = callback
        self.cache_interface = cache_interface

    def render(self, request: Request):
        request.setHeader(b'Content-Type', b'application/json')

        if self.config.get('set_cors'):
            request.setHeader(b'Access-Control-Allow-Origin', b'*')
            request.setHeader(b'Access-Control-Allow-Methods', b'GET,POST,PUT,DELETE')
            request.setHeader(b'Access-Control-Allow-Headers', b'x-prototype-version,x-requested-with')
            request.setHeader(b'Access-Control-Max-Age', b'2520')

        callback_name = 'response_%s_process' % request.method.lower().decode('ascii')
        callback = getattr(self, callback_name, None)
        if not callback:
            request.setResponseCode(NOT_ALLOWED)
            return b'Method not allowed'

        session_id = request.getSession().uid
        d = self.cache_interface.get(session_id)
        d.addCallback(callback, session_id, request)

        return NOT_DONE_YET

    def read_json_body(self, request: Request):
        try:
            result = ujson.loads(request.content.getvalue())
        except ValueError:
            request.setResponseCode(BAD_REQUEST)
            request.write(ujson.dumps({'status': 'error', 'message': 'Invalid JSON'}).encode('utf-8'))
            request.finish()
            result = None

        return result


class AuthApi(SessionResource):
    isLeaf = True

    @inlineCallbacks
    def response_get_process(self, session: Dict, session_id: str, request: Request):
        request.setResponseCode(OK)
        if session is None:
            session = {'auth': False}
            yield self.cache_interface.set(session_id, session)
        request.write(ujson.dumps(session).encode('utf-8'))
        request.finish()
        return None

    @inlineCallbacks
    def response_post_process(self, session: Dict, session_id: str, request: Request):
        body = self.read_json_body(request)

        if session and session.get('auth', False) is True:
            request.setResponseCode(OK)
            request.write(ujson.dumps({'status': 'success'}).encode('utf-8'))
        elif body and body.get('login') and body.get('password'):
            password_hash = hashlib.md5(body.get('password').encode('utf-8')).hexdigest()
            selector = Selector(Target.user, {'login': body.get('login'), 'password_hash': password_hash}, 1)
            database_request = DataBasePackage(Method.select, selector=selector)
            response = yield self.db_interface(database_request)
            result = DataBaseResponse.deserialize(response)
            if result.length == 0:
                request.setResponseCode(UNAUTHORIZED)
                request.write(ujson.dumps({'status': 'error', 'message': 'No match'}).encode('utf-8'))
            else:
                user = result.objects[0]
                session = {'login': user.login, 'auth': True}
                yield self.cache_interface.set(session_id, session)
                request.write(ujson.dumps({'status': 'success'}).encode('utf-8'))
        else:
            request.setResponseCode(BAD_REQUEST)
            request.write(ujson.dumps({'status': 'error', 'message': 'Invalid request'}).encode('utf-8'))

        request.finish()
        return None

    @inlineCallbacks
    def response_delete_process(self, session: Dict, session_id: str, request: Request):
        if session is None or session.get('auth', False) is not True:
            request.setResponseCode(UNAUTHORIZED)
            request.write(ujson.dumps({'status': 'error', 'message': 'Unauthorized'}).encode('utf-8'))
        else:
            yield self.cache_interface.set(session_id, {'auth': False})
            request.setResponseCode(OK)
            request.write(ujson.dumps({'status': 'success'}).encode('utf-8'))

        request.finish()
        return None


class UsersApi(SessionResource):
    isLeaf = True

    def response_get_process(self, session: Dict, session_id: str, request: Request):
        print(rerequest.path)
        request.setResponseCode(OK)
        request.write(b'test')
        request.finish()
        return None


class GeoApi(Resource):
    isLeaf = True

    def __init__(self, config, callback, cache_interface):
        super(GeoApi, self).__init__()
        self.config = config
        self.db_interface = callback

    def render_GET(self, request):
        # TODO: Parse request
        selector = Selector(Target.geo, {})
        api_request = DataBasePackage(Method.select, selector=selector)

        d = self.db_interface(api_request)
        d.addCallback(self.proto_response, request)

        return NOT_DONE_YET

    def proto_response(self, response: str, request):
        db_response = DataBaseResponse.deserialize(response)
        result = [{'lat': obj.latitude, 'lng': obj.longitude, 'speed': obj.speed,
                   'altitude': obj.altitude, 'stamp': obj.timestamp}
                  for obj in db_response.objects]

        request.setResponseCode(OK)
        request.write(ujson.dumps(result).encode('utf-8'))
        request.finish()


class WebApi(Resource):
    def __init__(self, config: Dict):
        super(WebApi, self).__init__()
        self.config = config

    def getChild(self, path: str, request: Request):
        if path == b'':
            return self
        return Resource.getChild(self, path, request)

    def render_GET(self, request):
        request.setResponseCode(OK)
        return ujson.dumps({'status': 'ok'}).encode('utf-8')


def initial(config, db_interface, cache_interface):
    api = WebApi(config)
    api.putChild(b'geo', GeoApi(config, db_interface, cache_interface))
    api.putChild(b'auth', AuthApi(config, db_interface, cache_interface))
    api.putChild(b'users', UsersApi(config, db_interface, cache_interface))

    site = Site(api)
    endpoint = TCP4ServerEndpoint(reactor, config.get('port', 80))
    endpoint.listen(site)
    print(' [x] Start Web API listen port: '+str(config.get('port', 80)))


def config_check(config):
    # TODO: Add checker
    return True
