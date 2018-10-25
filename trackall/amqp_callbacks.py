# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

from random import random
from six.moves import queue as six_queue
from twisted.internet.defer import inlineCallbacks, returnValue

from trackall.amqp import AMQPClient, ReplyToCheckerContext


class QueueCallback(object):
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
            except six_queue.Empty:
                returnValue(None)
            try:
                yield amqp_instance.publish(self.exchange, self.routing_key, message)
            except Exception:
                self.queue.put(message)
                raise


class ListenReplyCallback(object):
    def __init__(self, queue_name, callback):
        self.queue_name = queue_name
        self.reply_callback = callback
        self.queue = None

    @inlineCallbacks
    def callback(self, amqp_instance):
        properties, message = yield amqp_instance.read(self)
        response = yield self.reply_callback(message)
        if response:
            yield amqp_instance.publish('', properties.reply_to, response)
        returnValue(None)


class RequestResponseCallback(object):
    def __init__(self, name, config):
        self.exchange = config.get('exchange', '')
        self.routing_key = config.get('routing_key', '')
        self.amqp_url = config.get('amqp_url')
        self.name = name

    @inlineCallbacks
    def amqp_callback(self, amqp_instance, message):
        queue_name = self.name + '_' + str(random())
        yield amqp_instance.channel.queue_declare(queue=queue_name, auto_delete=True, exclusive=False)
        query_queue, _ = yield amqp_instance.channel.basic_consume(queue=queue_name, no_ack=True)
        yield amqp_instance.publish(self.exchange, self.routing_key, message, reply_to=queue_name)

        query_queue_context = ReplyToCheckerContext(queue_name, query_queue)
        _, message = yield amqp_instance.read(query_queue_context)
        returnValue(message)

    @inlineCallbacks
    def api_callback(self, message):
        amqp_connection = AMQPClient(
            self.amqp_url,
            self.amqp_callback,
            self.name
        )
        result = yield amqp_connection.run(message=message)
        returnValue(result)
