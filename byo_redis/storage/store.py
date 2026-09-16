"""In-memory Redis-like keyspace (DB0 only)."""

from __future__ import annotations

from collections import deque
from typing import Iterable, Iterator

from .expiry import is_expired, now_ms, ttl_seconds
from .types import KeyEntry, RedisType


class WrongTypeError(Exception):
    """Raised when an operation targets the wrong Redis type."""


class Store:
    """Single-database key-value store with lazy (and optional active) expiry."""

    def __init__(self) -> None:
        self._entries: dict[bytes, KeyEntry] = {}

    # ---- introspection -------------------------------------------------
    def __len__(self) -> int:
        self._purge_expired_all_lazy_touch()
        return len(self._entries)

    def keys(self) -> list[bytes]:
        self.active_expire_cycle(sample_size=len(self._entries) or 1)
        return list(self._entries.keys())

    def iter_entries(self) -> Iterator[tuple[bytes, KeyEntry]]:
        """Yield live entries (skips expired, deletes them)."""
        now = now_ms()
        expired: list[bytes] = []
        for key, entry in self._entries.items():
            if is_expired(entry.expire_at_ms, now=now):
                expired.append(key)
            else:
                yield key, entry
        for key in expired:
            self._entries.pop(key, None)

    def snapshot(self) -> dict[bytes, KeyEntry]:
        """Deep-copy live entries for persistence / replication."""
        return {key: entry.copy() for key, entry in self.iter_entries()}

    def clear(self) -> None:
        self._entries.clear()

    def load_entries(self, entries: dict[bytes, KeyEntry]) -> None:
        self._entries = {k: v.copy() for k, v in entries.items()}

    # ---- generic key ops -----------------------------------------------
    def delete(self, *keys: bytes) -> int:
        removed = 0
        for key in keys:
            entry = self._entries.get(key)
            if entry is None:
                continue
            if is_expired(entry.expire_at_ms):
                del self._entries[key]
                continue
            del self._entries[key]
            removed += 1
        return removed

    def exists(self, key: bytes) -> bool:
        return self._touch(key) is not None

    def get_entry(self, key: bytes) -> KeyEntry | None:
        return self._touch(key)

    def set_entry(self, key: bytes, entry: KeyEntry) -> None:
        self._entries[key] = entry

    def expire(self, key: bytes, seconds: int) -> int:
        entry = self._touch(key)
        if entry is None:
            return 0
        if seconds <= 0:
            del self._entries[key]
            return 1
        entry.expire_at_ms = now_ms() + seconds * 1000
        return 1

    def pexpire_at(self, key: bytes, expire_at_ms: int | None) -> None:
        entry = self._touch(key)
        if entry is None:
            return
        entry.expire_at_ms = expire_at_ms

    def ttl(self, key: bytes) -> int:
        entry = self._entries.get(key)
        if entry is None:
            return -2
        if is_expired(entry.expire_at_ms):
            del self._entries[key]
            return -2
        return ttl_seconds(entry.expire_at_ms)

    def type_of(self, key: bytes) -> RedisType | None:
        entry = self._touch(key)
        return None if entry is None else entry.type

    # ---- string --------------------------------------------------------
    def set_string(self, key: bytes, value: bytes, ex_seconds: int | None = None) -> None:
        expire_at_ms = None
        if ex_seconds is not None:
            if ex_seconds <= 0:
                # SET with EX<=0: store then immediately expire → effectively delete
                self._entries.pop(key, None)
                return
            expire_at_ms = now_ms() + ex_seconds * 1000
        self._entries[key] = KeyEntry(
            type=RedisType.STRING, value=value, expire_at_ms=expire_at_ms
        )

    def get_string(self, key: bytes) -> bytes | None:
        entry = self._touch(key)
        if entry is None:
            return None
        if entry.type is not RedisType.STRING:
            raise WrongTypeError
        return entry.value  # type: ignore[return-value]

    # ---- list ----------------------------------------------------------
    def lpush(self, key: bytes, elements: Iterable[bytes]) -> int:
        entry = self._touch(key)
        if entry is None:
            dq: deque[bytes] = deque()
            entry = KeyEntry(type=RedisType.LIST, value=dq)
            self._entries[key] = entry
        elif entry.type is not RedisType.LIST:
            raise WrongTypeError
        else:
            dq = entry.value  # type: ignore[assignment]
        for el in elements:
            dq.appendleft(el)
        return len(dq)

    def rpop(self, key: bytes) -> bytes | None:
        entry = self._touch(key)
        if entry is None:
            return None
        if entry.type is not RedisType.LIST:
            raise WrongTypeError
        dq: deque[bytes] = entry.value  # type: ignore[assignment]
        if not dq:
            del self._entries[key]
            return None
        value = dq.pop()
        if not dq:
            del self._entries[key]
        return value

    # ---- hash ----------------------------------------------------------
    def hset(self, key: bytes, items: list[tuple[bytes, bytes]]) -> int:
        entry = self._touch(key)
        if entry is None:
            mapping: dict[bytes, bytes] = {}
            entry = KeyEntry(type=RedisType.HASH, value=mapping)
            self._entries[key] = entry
        elif entry.type is not RedisType.HASH:
            raise WrongTypeError
        else:
            mapping = entry.value  # type: ignore[assignment]
        added = 0
        for field, value in items:
            if field not in mapping:
                added += 1
            mapping[field] = value
        return added

    def hgetall(self, key: bytes) -> list[bytes]:
        entry = self._touch(key)
        if entry is None:
            return []
        if entry.type is not RedisType.HASH:
            raise WrongTypeError
        mapping: dict[bytes, bytes] = entry.value  # type: ignore[assignment]
        out: list[bytes] = []
        for field, value in mapping.items():
            out.append(field)
            out.append(value)
        return out

    # ---- expiry maintenance --------------------------------------------
    def active_expire_cycle(self, sample_size: int = 20) -> int:
        """Randomly sample keys and delete expired ones. Returns deleted count."""
        if not self._entries:
            return 0
        import random

        keys = list(self._entries.keys())
        if not keys:
            return 0
        sample = keys if len(keys) <= sample_size else random.sample(keys, sample_size)
        deleted = 0
        now = now_ms()
        for key in sample:
            entry = self._entries.get(key)
            if entry is not None and is_expired(entry.expire_at_ms, now=now):
                del self._entries[key]
                deleted += 1
        return deleted

    def _purge_expired_all_lazy_touch(self) -> None:
        now = now_ms()
        expired = [
            key
            for key, entry in self._entries.items()
            if is_expired(entry.expire_at_ms, now=now)
        ]
        for key in expired:
            del self._entries[key]

    def _touch(self, key: bytes) -> KeyEntry | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if is_expired(entry.expire_at_ms):
            del self._entries[key]
            return None
        return entry

    def dbsize(self) -> int:
        self._purge_expired_all_lazy_touch()
        return len(self._entries)
