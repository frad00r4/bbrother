# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import math
import ujson

from typing import TypeVar, Generic

from bbrother.objects.tracker import Tracker


GeoPointType = TypeVar('GeoPointType')


class GeoPoint(Generic[GeoPointType]):
    def __init__(self, latitude: float, longitude: float, altitude: float, timestamp: int=None,
                 speed: float=None, tracker: Tracker=None, **kwargs) -> None:
        self.latitude = latitude
        self.longitude = longitude
        self.altitude = altitude
        self.timestamp = timestamp
        self.tracker = tracker
        self.speed = speed
        self.extra = kwargs

    def __getattr__(self, item):
        return self.extra.get(item)

    def set_speed_by_prev_point(self, point: GeoPointType) -> float:
        self.speed = math.sqrt(
            (self.latitude - point.latitude) ** 2
            + (self.longitude - point.longitude) ** 2
            + (self.altitude - point.altitude) ** 2
        )
        return self.speed

    def serialize(self) -> str:
        return ujson.dumps({'latitude': self.latitude,
                            'longitude': self.longitude,
                            'altitude': self.altitude,
                            'timestamp': self.timestamp,
                            'tracker': self.tracker.serialize() if self.tracker else None,
                            'speed': self.speed,
                            'extra': self.extra})

    @classmethod
    def deserialize(cls, json: str) -> GeoPointType:
        data = ujson.loads(json)
        return cls(
            data['latitude'],
            data['longitude'],
            data['altitude'],
            data['timestamp'],
            data['speed'],
            Tracker.deserialize(data['tracker']) if data['tracker'] else None,
            **data['extra']
        )

    def __str__(self) -> str:
        return 'Geo: {},{} Altitude: {} Time: {} Speed: {}'.format(
            self.latitude, self.longitude, self.altitude, self.timestamp, self.speed)
