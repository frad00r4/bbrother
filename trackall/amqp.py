# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

try:
    from Queue import Empty
except ImportError:
    from queue import Empty

import ujson

from twisted.internet import task
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.error import ConnectionRefusedError
from twisted.internet.protocol import ClientCreator
from pika.adapters.twisted_connection import TwistedProtocolConnection
from pika.connection import URLParameters
from pika.exceptions import ChannelClosed
from pika.spec import BasicProperties

from trackall.main import reactor


class AMQPClient(object):
    def __init__(self, url, queues):
        self._parameters = URLParameters(url)
        self._client = ClientCreator(reactor, TwistedProtocolConnection, self._parameters)
        self._queues = queues

    def error_connection_callback(self, failure):
        failure.trap(ConnectionRefusedError)
        print(' [!] Connection failed: ' + failure.getErrorMessage())
        # Try to reconnect
        reactor.callLater(2, self.connect)

    def error_channel_callback(self, failure):
        failure.trap(ChannelClosed)
        print(' [!] Connection failed: ' + failure.getErrorMessage())
        # Try to reconnect
        reactor.callLater(2, self.connect)

    def connect(self):
        d = self._client.connectTCP(self._parameters.host, self._parameters.port)
        d.addCallbacks(lambda protocol: protocol.ready, self.error_connection_callback)
        d.addCallback(self.run_loop)

    @inlineCallbacks
    def run_loop(self, connection):
        if not connection:
            returnValue(None)
        print(' [x] Connected to AMQP publisher')

        for queue in self._queues:
            channel = yield connection.channel()
            loop = task.LoopingCall(self.publish, channel, queue)
            d = loop.start(0.1)
            d.addErrback(self.error_channel_callback)

        returnValue(None)

    @inlineCallbacks
    def publish(self, channel, queue):
        while True:
            message = ''
            try:
                message = queue.get_nowait()
                print(message)
            except Empty:
                returnValue(None)
            properties = BasicProperties()
            yield channel.basic_publish('', 'test_queue', ujson.dumps(message), properties)


class AMQPClientReader(object):
    def __init__(self, url, callback):
        self._parameters = URLParameters(url)
        self._client = ClientCreator(reactor, TwistedProtocolConnection, self._parameters)
        self._db_callback = callback

    def error_connection_callback(self, failure):
        failure.trap(ConnectionRefusedError)
        print(' [!] Connection failed: ' + failure.getErrorMessage())
        # Try to reconnect
        reactor.callLater(2, self.connect)

    def error_channel_callback(self, failure):
        failure.trap(ChannelClosed)
        print(' [!] Connection failed: ' + failure.getErrorMessage())
        # Try to reconnect
        reactor.callLater(2, self.connect)

    def connect(self):
        d = self._client.connectTCP(self._parameters.host, self._parameters.port)
        d.addCallbacks(lambda protocol: protocol.ready, self.error_connection_callback)
        d.addCallback(self.run_loop)

    @inlineCallbacks
    def run_loop(self, connection):
        if not connection:
            returnValue(None)
        print(' [x] Connected to AMQP listener')

        channel = yield connection.channel()
        queue, _ = yield channel.basic_consume(queue='test_queue', no_ack=True)
        loop = task.LoopingCall(self.read, queue)
        d = loop.start(0.1)

        d.addErrback(self.error_channel_callback)

        returnValue(None)

    @inlineCallbacks
    def read(self, queue):
        channel, method, properties, body = yield queue.get()
        message = None
        try:
            message = ujson.loads(body)
        except ValueError:
            print('invalid JSON: {}'.format(body))
            returnValue(None)
        yield self._db_callback(message)
        # yield channel.basic_ack(delivery_tag=method.delivery_tag)
        returnValue(None)
