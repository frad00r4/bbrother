# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

from random import random
from twisted.internet.defer import inlineCallbacks

from trackall.objects.db_proto import DataBasePackage
from trackall.amqp import ReplyToCheckerContext


class ListenReplyCallback(object):
    def __init__(self, queue_name, callback):
        self.queue_name = queue_name
        self.reply_callback = callback
        self.queue = None

    @inlineCallbacks
    def callback(self, amqp_instance, channel):
        properties, message = yield amqp_instance.read(self, channel)
        response = yield self.reply_callback(message)
        yield amqp_instance.publish(channel, '', properties.reply_to, response)
        return None


class RequestResponseCallback(object):
    def __init__(self, name, config):
        self.exchange = config.get('exchange', '')
        self.routing_key = config.get('routing_key', '')
        self.amqp_url = config.get('amqp_url')
        self.name = name

    @inlineCallbacks
    def callback(self, amqp_instance, message: DataBasePackage):
        queue_name = self.name + '_' + str(random())
        channel = yield amqp_instance.connection.channel()

        yield channel.queue_declare(queue=queue_name, auto_delete=True, exclusive=False)
        query_queue, _ = yield channel.basic_consume(queue=queue_name, no_ack=True)
        query_queue_context = ReplyToCheckerContext(queue_name, query_queue)

        yield amqp_instance.publish(channel, self.exchange, self.routing_key, message, reply_to=queue_name)

        _, message = yield amqp_instance.read(query_queue_context, channel)
        channel.close()
        return message
