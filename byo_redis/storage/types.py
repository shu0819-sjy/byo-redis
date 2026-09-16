"""Key value type definitions."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any


class RedisType(Enum):
    STRING = auto()
    LIST = auto()
    HASH = auto()


@dataclass
class KeyEntry:
    type: RedisType
    value: Any  # bytes | deque[bytes] | dict[bytes, bytes]
    expire_at_ms: int | None = None

    def copy(self) -> "KeyEntry":
        if self.type is RedisType.STRING:
            value: Any = self.value
        elif self.type is RedisType.LIST:
            value = deque(self.value)
        elif self.type is RedisType.HASH:
            value = dict(self.value)
        else:  # pragma: no cover
            value = self.value
        return KeyEntry(type=self.type, value=value, expire_at_ms=self.expire_at_ms)
