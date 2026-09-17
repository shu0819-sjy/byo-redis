"""Expiry / TTL unit tests."""

from __future__ import annotations

import time

import pytest

from byo_redis.storage.store import Store


def test_allkeys_lru_evicts_least_recently_used_key() -> None:
    """allkeys-lru 应淘汰最久未访问的键。"""
    store = Store(maxmemory_bytes=140, maxmemory_policy="allkeys-lru")
    store.set_string(b"old", b"x" * 20)
    store.set_string(b"new", b"y" * 20)
    assert store.get_string(b"new") == b"y" * 20
    evicted = store.enforce_memory_limit()
    assert evicted == [b"old"]
    assert store.get_string(b"old") is None
    assert store.get_string(b"new") == b"y" * 20


def test_noeviction_raises_without_removing_data() -> None:
    """noeviction 超限时抛错且保留原键空间。"""
    from byo_redis.storage.store import MemoryLimitError

    store = Store(maxmemory_bytes=1, maxmemory_policy="noeviction")
    store.set_string(b"k", b"v")
    with pytest.raises(MemoryLimitError):
        store.enforce_memory_limit()
    assert store.get_string(b"k") == b"v"


def test_lazy_expire_on_get() -> None:
    store = Store()
    store.set_string(b"k", b"v", ex_seconds=1)
    assert store.get_string(b"k") == b"v"
    time.sleep(1.05)
    assert store.get_string(b"k") is None


def test_expire_command_and_ttl() -> None:
    store = Store()
    store.set_string(b"k", b"v")
    assert store.ttl(b"k") == -1
    assert store.expire(b"k", 2) == 1
    ttl = store.ttl(b"k")
    assert 0 <= ttl <= 2
    assert store.expire(b"missing", 1) == 0
    assert store.ttl(b"missing") == -2


def test_expire_zero_deletes() -> None:
    store = Store()
    store.set_string(b"k", b"v")
    assert store.expire(b"k", 0) == 1
    assert store.get_string(b"k") is None


def test_active_expire_cycle() -> None:
    store = Store()
    store.set_string(b"a", b"1", ex_seconds=1)
    store.set_string(b"b", b"2")
    time.sleep(1.05)
    deleted = store.active_expire_cycle(sample_size=10)
    assert deleted >= 1
    assert store.get_string(b"a") is None
    assert store.get_string(b"b") == b"2"
