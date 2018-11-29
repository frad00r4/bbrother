# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

from yaml import load

__all__ = ['config']


with open('settings/default.yaml', 'r') as stream:
    config = load(stream)
