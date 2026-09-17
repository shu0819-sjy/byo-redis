"""In-memory Redis-like keyspace (DB0 only)."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Iterator

from .expiry import is_expired, now_ms, ttl_seconds
from .types import KeyEntry, RedisType


class WrongTypeError(Exception):
    """Raised when an operation targets the wrong Redis type."""


class MemoryLimitError(Exception):
    """Raised when maxmemory is exceeded and no key can be evicted."""


class Store:
    """Single-database key-value store with lazy (and optional active) expiry."""

    def __init__(
        self,
        *,
        maxmemory_bytes: int = 0,
        maxmemory_policy: str = "noeviction",
    ) -> None:
        self._entries: dict[bytes, KeyEntry] = {}
        self.maxmemory_bytes = maxmemory_bytes
        self.maxmemory_policy = maxmemory_policy
        self._access_clock = 0
        self._last_access: dict[bytes, int] = {}
        self.evicted_keys = 0

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
            self._last_access.pop(key, None)

    def snapshot(self) -> dict[bytes, KeyEntry]:
        """Deep-copy live entries for persistence / replication."""
        return {key: entry.copy() for key, entry in self.iter_entries()}

    def clear(self) -> None:
        self._entries.clear()
        self._last_access.clear()

    def load_entries(self, entries: dict[bytes, KeyEntry]) -> None:
        self._entries = {k: v.copy() for k, v in entries.items()}
        self._last_access = {key: index for index, key in enumerate(self._entries)}
        self._access_clock = len(self._last_access)

    # ---- generic key ops -----------------------------------------------
    def delete(self, *keys: bytes) -> int:
        removed = 0
        for key in keys:
            entry = self._entries.get(key)
            if entry is None:
                continue
            if is_expired(entry.expire_at_ms):
                del self._entries[key]
                self._last_access.pop(key, None)
                continue
            del self._entries[key]
            self._last_access.pop(key, None)
            removed += 1
        return removed

    def exists(self, key: bytes) -> bool:
        return self._touch(key) is not None

    def get_entry(self, key: bytes) -> KeyEntry | None:
        return self._touch(key)

    def set_entry(self, key: bytes, entry: KeyEntry) -> None:
        self._entries[key] = entry
        self._touch_access(key)

    def expire(self, key: bytes, seconds: int) -> int:
        entry = self._touch(key)
        if entry is None:
            return 0
        if seconds <= 0:
            del self._entries[key]
            self._last_access.pop(key, None)
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
            self._last_access.pop(key, None)
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
                self._last_access.pop(key, None)
                return
            expire_at_ms = now_ms() + ex_seconds * 1000
        self._entries[key] = KeyEntry(
            type=RedisType.STRING, value=value, expire_at_ms=expire_at_ms
        )
        self._touch_access(key)

    def get_string(self, key: bytes) -> bytes | None:
        entry = self._touch(key)
        if entry is None:
            return None
        if entry.type is not RedisType.STRING:
            raise WrongTypeError
        assert isinstance(entry.value, bytes)
        return entry.value

    # ---- list ----------------------------------------------------------
    def lpush(self, key: bytes, elements: Iterable[bytes]) -> int:
        entry = self._touch(key)
        if entry is None:
            dq: deque[bytes] = deque()
            entry = KeyEntry(type=RedisType.LIST, value=dq)
            self._entries[key] = entry
            self._touch_access(key)
        elif entry.type is not RedisType.LIST:
            raise WrongTypeError
        else:
            assert isinstance(entry.value, deque)
            dq = entry.value
        for el in elements:
            dq.appendleft(el)
        return len(dq)

    def rpop(self, key: bytes) -> bytes | None:
        entry = self._touch(key)
        if entry is None:
            return None
        if entry.type is not RedisType.LIST:
            raise WrongTypeError
        assert isinstance(entry.value, deque)
        dq = entry.value
        if not dq:
            del self._entries[key]
            self._last_access.pop(key, None)
            return None
        value = dq.pop()
        if not dq:
            del self._entries[key]
            self._last_access.pop(key, None)
        return value

    # ---- hash ----------------------------------------------------------
    def hset(self, key: bytes, items: list[tuple[bytes, bytes]]) -> int:
        entry = self._touch(key)
        if entry is None:
            mapping: dict[bytes, bytes] = {}
            entry = KeyEntry(type=RedisType.HASH, value=mapping)
            self._entries[key] = entry
            self._touch_access(key)
        elif entry.type is not RedisType.HASH:
            raise WrongTypeError
        else:
            assert isinstance(entry.value, dict)
            mapping = entry.value
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
        assert isinstance(entry.value, dict)
        mapping = entry.value
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
                self._last_access.pop(key, None)
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
            self._last_access.pop(key, None)

    def _touch(self, key: bytes) -> KeyEntry | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if is_expired(entry.expire_at_ms):
            del self._entries[key]
            self._last_access.pop(key, None)
            return None
        self._touch_access(key)
        return entry

    def dbsize(self) -> int:
        self._purge_expired_all_lazy_touch()
        return len(self._entries)

    def keyspace_stats(self) -> tuple[int, int, int]:
        """返回实时键数、带 TTL 键数和平均剩余 TTL 毫秒数。"""
        self._purge_expired_all_lazy_touch()
        current = now_ms()
        ttl_values = [
            max(0, entry.expire_at_ms - current)
            for entry in self._entries.values()
            if entry.expire_at_ms is not None
        ]
        avg_ttl = 0 if not ttl_values else sum(ttl_values) // len(ttl_values)
        return len(self._entries), len(ttl_values), avg_ttl

    def memory_usage(self) -> int:
        """返回键空间的近似字节数，不包含 Python 容器共享开销。"""
        self._purge_expired_all_lazy_touch()
        total = 0
        for key, entry in self._entries.items():
            total += len(key) + 64
            if entry.type is RedisType.STRING:
                assert isinstance(entry.value, bytes)
                total += len(entry.value)
            elif entry.type is RedisType.LIST:
                assert isinstance(entry.value, deque)
                total += sum(len(item) + 16 for item in entry.value)
            else:
                assert isinstance(entry.value, dict)
                total += sum(len(field) + len(value) + 32 for field, value in entry.value.items())
        return total

    def enforce_memory_limit(self) -> list[bytes]:
        """按策略淘汰键；noeviction 无法满足上限时抛出 MemoryLimitError。"""
        if self.maxmemory_bytes <= 0:
            return []
        evicted: list[bytes] = []
        while self.memory_usage() > self.maxmemory_bytes:
            candidates = [
                key
                for key, entry in self._entries.items()
                if self.maxmemory_policy != "volatile-ttl" or entry.expire_at_ms is not None
            ]
            if not candidates:
                raise MemoryLimitError("OOM command not allowed when used memory > maxmemory")
            if self.maxmemory_policy == "volatile-ttl":
                key = min(
                    candidates,
                    key=lambda item: self._entries[item].expire_at_ms or 0,
                )
            elif self.maxmemory_policy == "allkeys-lru":
                key = min(candidates, key=lambda item: self._last_access.get(item, 0))
            else:
                raise MemoryLimitError("OOM command not allowed when used memory > maxmemory")
            del self._entries[key]
            self._last_access.pop(key, None)
            self.evicted_keys += 1
            evicted.append(key)
        return evicted

    def _touch_access(self, key: bytes) -> None:
        """更新键的访问时钟，供 allkeys-lru 淘汰策略选择最久未访问键。"""
        self._access_clock += 1
        self._last_access[key] = self._access_clock
