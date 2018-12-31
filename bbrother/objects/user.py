# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import ujson

from typing import TypeVar, Generic


UserType = TypeVar('UserType')


class User(Generic[UserType]):
    def __init__(self, user_id: int, login: str, password_hash: str, **kwargs) -> None:
        self.user_id = user_id
        self.login = login
        self.password_hash = password_hash
        self.extras = kwargs

    def serialize(self) -> str:
        return ujson.dumps({'user_id': self.user_id, 'login': self.login,
                            'password_hash': self.password_hash, 'extras': self.extras})

    @classmethod
    def deserialize(cls, json: str) -> UserType:
        data = ujson.loads(json)
        return cls(data['user_id'], data['login'], data['password_hash'], **data['extras'])

    def __str__(self) -> str:
        return 'User ID {} -> {}'.format(self.user_id, self.login)
