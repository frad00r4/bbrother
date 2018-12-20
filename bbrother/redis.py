# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

from txredis.client import RedisClient
from twisted.internet.protocol import ClientCreator

from bbrother.main import reactor


def initial():
    client = ClientCreator(reactor, RedisClient)
