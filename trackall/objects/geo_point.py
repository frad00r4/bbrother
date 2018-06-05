# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import math

from trackall.exceptions import WrongGeoPoint


class GeoPoint(object):
    def __init__(self, latitude, longitude, altitude, timestamp, **kwargs):
        self.latitude = latitude
        self.longitude = longitude
        self.altitude = altitude
        self.timestamp = timestamp
        self.speed = kwargs['speed'] if 'speed' in kwargs else None
        self.extra = kwargs

    def __getattr__(self, item):
        return self.extra[item] if item in self.extra else None

    def set_speed_by_prev_point(self, point):
        if not isinstance(point, GeoPoint):
            raise WrongGeoPoint('The point is not GeoPoint')
        self.speed = math.sqrt((self.latitude - point.latitude) ** 2
                               + (self.longitude - point.longitude) ** 2
                               + (self.altitude - point.altitude) ** 2)
        return self.speed
