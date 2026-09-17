"""Key value type definitions."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum, auto

RedisValue = bytes | deque[bytes] | dict[bytes, bytes]


class RedisType(Enum):
    STRING = auto()
    LIST = auto()
    HASH = auto()


@dataclass
class KeyEntry:
    type: RedisType
    value: RedisValue
    expire_at_ms: int | None = None

    def copy(self) -> KeyEntry:
        if isinstance(self.value, deque):
            value: RedisValue = deque(self.value)
        elif isinstance(self.value, dict):
            value = dict(self.value)
        else:
            value = self.value
        return KeyEntry(type=self.type, value=value, expire_at_ms=self.expire_at_ms)
