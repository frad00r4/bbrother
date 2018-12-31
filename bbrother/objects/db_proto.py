# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import ujson

from enum import Enum
from typing import TypeVar, Generic, Dict, List

from bbrother.objects.geo_point import GeoPoint
from bbrother.objects.tracker import Tracker
from bbrother.objects.user import User


TargetTypes = TypeVar('TargetTypes', GeoPoint, Tracker, User)
DataBasePackageType = TypeVar('DataBasePackageType')
SelectorType = TypeVar('SelectorType')
DataBaseResponseType = TypeVar('DataBaseResponseType')


class Method(Enum):
    select = 0
    insert = 1
    update = 2
    delete = 3


class Target(Enum):
    geo = GeoPoint
    tracker = Tracker
    user = User


class Selector(Generic[SelectorType]):
    def __init__(self, target: Target, selector: Dict, limit: int = None, offset: int = None) -> None:
        self.target = target
        self.selector = selector
        self.limit = limit
        self.offset = offset

    def serialize(self) -> str:
        return ujson.dumps({'target': self.target.name,
                            'selector': self.selector,
                            'limit': self.limit,
                            'offset': self.offset})

    @classmethod
    def deserialize(cls, json: str) -> SelectorType:
        data = ujson.loads(json)
        return cls(
            Target[data['target']],
            data['selector'],
            data['limit'],
            data['offset']
        )


class DataBasePackage(Generic[DataBasePackageType]):
    def __init__(self, method: Method, target: TargetTypes = None,
                 selector: Selector = None, silent: bool = False) -> None:
        self.method = method
        self.target_object = Target(type(target)) if target else None
        self.target = target
        self.selector = selector
        self.silent = silent

    def serialize(self) -> str:
        return ujson.dumps({'method': self.method.value,
                            'target': self.target.serialize() if self.target else None,
                            'target_object': self.target_object.name if self.target_object else None,
                            'selector': self.selector.serialize() if self.selector else None,
                            'silent': self.silent})

    @classmethod
    def deserialize(cls, json: str) -> DataBasePackageType:
        data = ujson.loads(json)
        return cls(
            Method(data['method']),
            Target[data['target_object']].value.deserialize(data['target']) if data['target'] else None,
            Selector.deserialize(data['selector']) if data['selector'] else None,
            data['silent']
        )

    def __str__(self) -> str:
        return 'Method: {}, Target: {}, Selector: {}'.format(
            self.method.name,
            self.target.serialize() if self.target else None,
            ujson.dumps(self.selector.serialize()))


class DataBaseResponse(Generic[DataBaseResponseType]):
    def __init__(self, objects: List[TargetTypes]) -> None:
        self.objects = objects
        self.length = len(self.objects)
        self.object_type = Target(type(self.objects[0])) if self.objects else None

    def serialize(self) -> str:
        return ujson.dumps({'objects': [obj.serialize() for obj in self.objects],
                            'object_type': self.object_type.name if self.object_type else None})

    @classmethod
    def deserialize(cls, json: str) -> DataBaseResponseType:
        data = ujson.loads(json)
        objects = [Target[data['object_type']].value.deserialize(obj_json)
                   for obj_json in data['objects']]
        return cls(objects)
