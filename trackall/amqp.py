# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import abc
import ujson
import six

from twisted.internet import task
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.error import ConnectionRefusedError
from twisted.internet.protocol import ClientCreator
from pika.adapters.twisted_connection import TwistedProtocolConnection
from pika.connection import URLParameters
from pika.exceptions import ChannelClosed
from pika.spec import BasicProperties

from trackall.main import reactor


@six.add_metaclass(abc.ABCMeta)
class AMQPClientAbstract(object):

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
        reactor.callLater(2, self.run)

    @inlineCallbacks
    def publish(self, exchange, routing_key, message, **kwargs):
        properties = BasicProperties(**kwargs)
        yield self.channel.basic_publish(exchange, routing_key, ujson.dumps(message), properties)
        returnValue(None)

    @inlineCallbacks
    def read(self, context):
        if context.queue is None:
            yield self.channel.queue_declare(queue=context.queue_name, auto_delete=True, exclusive=False)
            context.queue, _ = yield self.channel.basic_consume(queue=context.queue_name, no_ack=True)

        _, _, properties, body = yield context.queue.get()
        message = None
        try:
            message = ujson.loads(body)
        except ValueError:
            print(' [!] AMQP {}.invalid JSON: {}'.format(self.name, body))
            returnValue((None, None))
        returnValue((properties, message))

    @abc.abstractmethod
    def run(self):
        raise NotImplementedError


class AMQPClientPermanent(AMQPClientAbstract):

    def run(self):
        d = self.client.connectTCP(self.parameters.host, self.parameters.port)
        d.addCallbacks(lambda protocol: protocol.ready, self.error_connection_callback)
        d.addCallback(self.run_loop)

    @inlineCallbacks
    def run_loop(self, connection):
        if not connection:
            returnValue(None)
        print(' [x] Connected to AMQP {} permanent'.format(self.name))

        self.channel = yield connection.channel()

        loop = task.LoopingCall(self.callback, self)
        d = loop.start(0.1)
        d.addErrback(self.error_connection_callback)

        returnValue(None)


class AMQPClient(AMQPClientAbstract):

    def run(self, *args, **kwargs):
        d = self.client.connectTCP(self.parameters.host, self.parameters.port)
        d.addCallbacks(lambda protocol: protocol.ready, self.error_connection_callback)
        d.addCallback(self.execute_callback, *args, **kwargs)
        return d

    @inlineCallbacks
    def execute_callback(self, connection, *args, **kwargs):
        if not connection:
            returnValue(None)
        print(' [x] Connected to AMQP {}'.format(self.name))

        self.channel = yield connection.channel()
        result = yield self.callback(self, *args, **kwargs)
        self.channel.close()
        self.channel = None
        connection.close()
        print(' [x] AMQP Disconnected {}'.format(self.name))

        returnValue(result)


class ReplyToCheckerContext(object):
    def __init__(self, queue_name, queue):
        self.queue = queue
        self.queue_name = queue_name
