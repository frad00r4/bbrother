# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import


class User(object):
    def __init__(self, user_id, **kwargs):
        self.user_id = user_id
        self.extras = kwargs

    def __getattr__(self, item):
        return self.extra[item] if item in self.extra else None
