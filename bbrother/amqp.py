# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import abc

from twisted.internet import task
from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ConnectionRefusedError
from twisted.internet.protocol import ClientCreator
from pika.adapters.twisted_connection import TwistedProtocolConnection
from pika.connection import URLParameters
from pika.exceptions import ChannelClosed
from pika.spec import BasicProperties

from bbrother.main import reactor


class AMQPClientAbstract(metaclass=abc.ABCMeta):

    def __init__(self, url: str, callback, name: str=''):
        self.parameters = URLParameters(url)
        self.client = ClientCreator(reactor, TwistedProtocolConnection, self.parameters)
        self.name = name
        self.connection = None
        self.callback = callback

    def error_connection_callback(self, failure):
        failure.trap(ConnectionRefusedError, ChannelClosed)
        print(' [!] Connection failed {}: {}'.format(self.name, failure.getErrorMessage()))
        # Try to reconnect
        reactor.callLater(2, self.run)

    @inlineCallbacks
    def publish(self, channel, exchange, routing_key, message, **kwargs):
        close_channel = False
        if channel is None:
            channel = yield self.connection.channel()
            close_channel = True

        properties = BasicProperties(**kwargs)
        yield channel.basic_publish(
            exchange,
            routing_key,
            message.serialize() if message else 'Done',
            properties
        )

        if close_channel is True:
            channel.close()

        return None

    @inlineCallbacks
    def read(self, context, channel=None):
        close_channel = False
        if channel is None:
            channel = yield self.connection.channel()
            close_channel = True

        if context.queue is None:
            yield channel.queue_declare(queue=context.queue_name, auto_delete=True, exclusive=False)
            context.queue, _ = yield channel.basic_consume(queue=context.queue_name, no_ack=True)

        _, _, properties, body = yield context.queue.get()

        if close_channel is True:
            channel.close()

        return properties, body

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
            return None
        print(' [x] Connected to AMQP {} permanent'.format(self.name))

        self.connection = yield connection
        channel = yield self.connection.channel()

        loop = task.LoopingCall(self.callback, self, channel)
        d = loop.start(0.1)
        d.addErrback(self.error_connection_callback)

        return None


class AMQPClient(AMQPClientAbstract):

    def run(self, *args, **kwargs):
        d = self.client.connectTCP(self.parameters.host, self.parameters.port)
        d.addCallbacks(lambda protocol: protocol.ready, self.error_connection_callback)
        d.addCallback(self.execute_callback, *args, **kwargs)
        return d

    @inlineCallbacks
    def execute_callback(self, connection, *args, **kwargs):
        if not connection:
            return None
        print(' [x] Connected to AMQP {}'.format(self.name))

        self.connection = yield connection
        channel = yield self.connection.channel()
        result = yield self.callback(self, channel, *args, **kwargs)
        channel.close()
        connection.close()
        print(' [x] AMQP Disconnected {}'.format(self.name))

        return result


class AMQPClientPermanentCallback(AMQPClientAbstract):
    def __init__(self, url: str, callback, name: str=''):
        super().__init__(url, callback, name)
        self.connection = None

    def run(self, *args, **kwargs):
        d = self.client.connectTCP(self.parameters.host, self.parameters.port)
        d.addCallbacks(lambda protocol: protocol.ready, self.error_connection_callback)
        d.addCallback(self.make_connection)
        return d

    def make_connection(self, connection):
        if not connection:
            return None
        print(' [x] Connected to AMQP {} permanent'.format(self.name))
        self.connection = connection

    @inlineCallbacks
    def run_callback(self, channel, *args, **kwargs):
        result = yield self.callback(self, channel, *args, **kwargs)
        return result


class ReplyToCheckerContext(object):
    def __init__(self, queue_name: str, queue):
        self.queue = queue
        self.queue_name = queue_name
