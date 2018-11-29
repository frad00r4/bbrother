# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import ujson

from enum import Enum
from typing import TypeVar, Generic


class IdentifyType(Enum):
    IMEI = 0


TrackerType = TypeVar('TrackerType')


class Tracker(Generic[TrackerType]):
    def __init__(self, id_type: IdentifyType, id_str: str) -> None:
        self.id_type = id_type
        self.id_str = id_str

    def serialize(self) -> str:
        return ujson.dumps({'id_type': self.id_type.value, 'id_str': self.id_str})

    @classmethod
    def deserialize(cls, json: str) -> TrackerType:
        data = ujson.loads(json)
        return cls(IdentifyType(data['id_type']), data['id_str'])

    def __str__(self) -> str:
        return '{}: {}'.format(self.id_type.name, self.id_str)
