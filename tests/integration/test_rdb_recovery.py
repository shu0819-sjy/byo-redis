"""Integration: RDB SAVE and restart recovery."""

from __future__ import annotations

import struct
from pathlib import Path

import pytest

from byo_redis.config import Config
from byo_redis.server import RedisServer
from tests.conftest import RespClient, unused_port


@pytest.mark.asyncio
async def test_rdb_save_and_reload(tmp_path: Path) -> None:
    port = unused_port()
    data_dir = tmp_path / "rdbdata"
    config = Config(
        host="127.0.0.1",
        port=port,
        dir=data_dir,
        aof_enabled=False,  # force RDB path
        log_level="warning",
    )
    server = RedisServer(config)
    await server.start()
    client = RespClient(config.host, config.port)
    await client.connect()
    try:
        assert await client.execute("SET", "str", "hello") == "OK"
        assert await client.execute("LPUSH", "lst", "a", "b") == 2
        assert await client.execute("HSET", "hs", "f", "v") == 1
        assert await client.execute("SET", "ttlkey", "t", "EX", "3600") == "OK"
        assert await client.execute("SAVE") == "OK"
        assert config.rdb_path.exists()
        assert config.rdb_path.stat().st_size > 0
    finally:
        await client.close()
        await server.stop()

    # Restart with same dir — AOF disabled, RDB should load
    server2 = RedisServer(config)
    await server2.start()
    client2 = RespClient(config.host, config.port)
    await client2.connect()
    try:
        assert await client2.execute("GET", "str") == b"hello"
        # list: LPUSH a b → left is b; RPOP right → a
        assert await client2.execute("RPOP", "lst") == b"a"
        flat = await client2.execute("HGETALL", "hs")
        assert isinstance(flat, list)
        mapping = dict(zip(flat[0::2], flat[1::2], strict=True))
        assert mapping == {b"f": b"v"}
        assert await client2.execute("GET", "ttlkey") == b"t"
    finally:
        await client2.close()
        await server2.stop()


@pytest.mark.asyncio
async def test_rdb_roundtrip_bytes() -> None:
    from byo_redis.persistence.rdb import dump_rdb_bytes, load_rdb_bytes
    from byo_redis.storage.store import Store

    store = Store()
    store.set_string(b"k", b"v")
    store.lpush(b"L", [b"1", b"2"])
    store.hset(b"H", [(b"a", b"b")])
    raw = dump_rdb_bytes(store)
    entries = load_rdb_bytes(raw)
    store2 = Store()
    store2.load_entries(entries)
    assert store2.get_string(b"k") == b"v"
    assert store2.rpop(b"L") == b"1"
    assert store2.hgetall(b"H") == [b"a", b"b"]


def test_rdb_checksum_detects_corruption() -> None:
    """RDB 任意主体字节被篡改后必须拒绝加载。"""
    from byo_redis.persistence.rdb import RdbError, dump_rdb_bytes, load_rdb_bytes
    from byo_redis.storage.store import Store

    store = Store()
    store.set_string(b"key", b"value")
    raw = bytearray(dump_rdb_bytes(store))
    raw[16] ^= 0x01
    with pytest.raises(RdbError, match="checksum"):
        load_rdb_bytes(bytes(raw))


def test_rdb_v2_loader_remains_compatible_with_v1_snapshot() -> None:
    """升级校验格式后仍能读取已有 BYOR v1 数据。"""
    from byo_redis.persistence.rdb import dump_rdb_bytes, load_rdb_bytes
    from byo_redis.storage.store import Store

    store = Store()
    store.set_string(b"legacy", b"value")
    version_two = dump_rdb_bytes(store)
    version_one = version_two[:4] + struct.pack("<I", 1) + version_two[8:-8] + version_two[-4:]
    entries = load_rdb_bytes(version_one)
    assert entries[b"legacy"].value == b"value"


def test_rdb_rejects_file_over_configured_load_limit(tmp_path: Path) -> None:
    """超出启动加载上限的 RDB 必须在读取和解析前被拒绝。"""
    from byo_redis.persistence.rdb import RdbError, load_rdb

    path = tmp_path / "oversized.rdb"
    path.write_bytes(b"x" * 1025)
    with pytest.raises(RdbError, match="exceeds load limit"):
        load_rdb(path, max_file_bytes=1024)


def test_aof_rejects_file_over_configured_load_limit(tmp_path: Path) -> None:
    """超出启动加载上限的 AOF 必须在整体读入内存前被拒绝。"""
    from byo_redis.persistence.aof import AofError, AOFLog

    path = tmp_path / "oversized.aof"
    path.write_bytes(b"x" * 1025)
    with pytest.raises(AofError, match="exceeds load limit"):
        AOFLog.read_commands(path, max_file_bytes=1024)
