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
    def __init__(self, url, callback, name=''):
        self.parameters = URLParameters(url)
        self.client = ClientCreator(reactor, TwistedProtocolConnection, self.parameters)
        self.name = name
        self.callback = callback
        self.channel = None

    def error_connection_callback(self, failure):
        failure.trap(ConnectionRefusedError, ChannelClosed)
        print(' [!] Connection failed {}: {}'.format(self.name, failure.getErrorMessage()))
        # Try to reconnect
        reactor.callLater(2, self.connect)

    def connect(self):
        d = self.client.connectTCP(self.parameters.host, self.parameters.port)
        d.addCallbacks(lambda protocol: protocol.ready, self.error_connection_callback)
        d.addCallback(self.run_loop)

    @inlineCallbacks
    def run_loop(self, connection):
        if not connection:
            returnValue(None)
        print(' [x] Connected to AMQP {}'.format(self.name))

        self.channel = yield connection.channel()

        loop = task.LoopingCall(self.callback, self)
        d = loop.start(0.1)
        d.addErrback(self.error_connection_callback)

        returnValue(None)

    @inlineCallbacks
    def publish(self, exchange, routing_key, message):
        properties = BasicProperties()
        yield self.channel.basic_publish(exchange, routing_key, ujson.dumps(message), properties)

    @inlineCallbacks
    def read(self, context):
        if context.queue is None:
            context.queue, _ = yield self.channel.basic_consume(queue=context.queue_name, no_ack=True)

        _, _, properties, body = yield context.queue.get()
        message = None
        try:
            message = ujson.loads(body)
        except ValueError:
            print(' [!] AMQP {}.invalid JSON: {}'.format(self.name, body))
            returnValue((None, None))
        returnValue((properties, message))


class GatePublisherCallback(object):
    def __init__(self, config, queue):
        self.exchange = config.get('exchange', '')
        self.routing_key = config.get('routing_key', '')
        self.queue = queue

    @inlineCallbacks
    def callback(self, amqp_instance):
        while True:
            message = ''
            try:
                message = self.queue.get_nowait()
            except Empty:
                returnValue(None)
            try:
                yield amqp_instance.publish(self.exchange, self.routing_key, message)
            except Exception:
                self.queue.put(message)
                raise


class DBInsertCallback(object):
    def __init__(self, config, callback):
        self.queue_name = config.get('queue')
        self.db_callback = callback
        self.queue = None

    @inlineCallbacks
    def callback(self, amqp_instance):
        properties, message = yield amqp_instance.read(self)
        yield self.db_callback(message)
        returnValue(None)
