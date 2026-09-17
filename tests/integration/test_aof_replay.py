"""Integration: AOF append and replay on restart."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from byo_redis.config import Config
from byo_redis.persistence.aof import AofError, AOFLog
from byo_redis.protocol.encoder import encode_command
from byo_redis.server import RedisServer
from tests.conftest import RespClient, unused_port


@pytest.mark.asyncio
async def test_aof_append_and_replay(tmp_path: Path) -> None:
    port = unused_port()
    data_dir = tmp_path / "aofdata"
    config = Config(
        host="127.0.0.1",
        port=port,
        dir=data_dir,
        aof_enabled=True,
        aof_fsync="always",
        log_level="warning",
    )
    server = RedisServer(config)
    await server.start()
    client = RespClient(config.host, config.port)
    await client.connect()
    try:
        assert await client.execute("SET", "a", "1") == "OK"
        assert await client.execute("LPUSH", "L", "x") == 1
        assert await client.execute("HSET", "H", "f", "v") == 1
        # reads should not be required for AOF growth check
        await client.execute("GET", "a")
        assert config.aof_path.exists()
        size = config.aof_path.stat().st_size
        assert size > 0
        content = config.aof_path.read_bytes()
        assert b"SET" in content
        assert b"GET" not in content  # read commands must not pollute AOF
    finally:
        await client.close()
        await server.stop()

    # Remove RDB if any so AOF is the recovery source
    if config.rdb_path.exists():
        config.rdb_path.unlink()

    server2 = RedisServer(config)
    await server2.start()
    client2 = RespClient(config.host, config.port)
    await client2.connect()
    try:
        assert await client2.execute("GET", "a") == b"1"
        assert await client2.execute("RPOP", "L") == b"x"
        flat = await client2.execute("HGETALL", "H")
        assert isinstance(flat, list)
        mapping = dict(zip(flat[0::2], flat[1::2], strict=True))
        assert mapping == {b"f": b"v"}
    finally:
        await client2.close()
        await server2.stop()


@pytest.mark.asyncio
async def test_aof_always_waits_for_fsync_before_reply(tmp_path: Path) -> None:
    """always 策略必须在客户端收到 OK 前完成 fsync。"""
    port = unused_port()
    config = Config(
        host="127.0.0.1",
        port=port,
        dir=tmp_path / "aof-always",
        aof_enabled=True,
        aof_fsync="always",
        log_level="warning",
    )
    server = RedisServer(config)
    await server.start()
    fsync_called = False
    original_fsync = server.aof._fsync_sync

    def mark_fsync() -> None:
        nonlocal fsync_called
        fsync_called = True
        original_fsync()

    server.aof._fsync_sync = mark_fsync
    client = RespClient(config.host, config.port)
    await client.connect()
    try:
        assert await client.execute("SET", "durable", "1") == "OK"
        assert fsync_called is True
    finally:
        await client.close()
        await server.stop()


def test_aof_ignores_only_incomplete_tail(tmp_path: Path) -> None:
    """完整命令可恢复，末尾半条命令只能被忽略。"""
    path = tmp_path / "tail.aof"
    path.write_bytes(encode_command([b"SET", b"k", b"v"]) + b"*3\r\n$3\r\nSET")
    assert AOFLog.read_commands(path) == [[b"SET", b"k", b"v"]]


def test_aof_rejects_corruption_in_file_body(tmp_path: Path) -> None:
    """中段协议损坏必须阻止启动，不能静默加载前缀。"""
    path = tmp_path / "corrupt.aof"
    path.write_bytes(encode_command([b"SET", b"k", b"v"]) + b"!invalid\r\n")
    with pytest.raises(AofError, match="protocol error"):
        AOFLog.read_commands(path)


@pytest.mark.asyncio
async def test_bgrewriteaof_compacts_and_recovers_state(tmp_path: Path) -> None:
    """BGREWRITEAOF 应压缩历史并保持字符串、列表、哈希和 TTL。"""
    config = Config(
        host="127.0.0.1",
        port=unused_port(),
        dir=tmp_path / "rewrite",
        aof_enabled=True,
        aof_fsync="always",
        log_level="warning",
    )
    server = RedisServer(config)
    await server.start()
    client = RespClient(config.host, config.port)
    await client.connect()
    try:
        for index in range(20):
            assert await client.execute("SET", "version", str(index)) == "OK"
        assert await client.execute("LPUSH", "items", "a", "b") == 2
        assert await client.execute("HSET", "meta", "field", "value") == 1
        assert await client.execute("EXPIRE", "version", "120") == 1
        before_size = config.aof_path.stat().st_size
        assert (
            await client.execute("BGREWRITEAOF")
            == "Background append only file rewriting started"
        )
        assert server._aof_rewrite_task is not None
        await server._aof_rewrite_task
        assert config.aof_path.stat().st_size < before_size
    finally:
        await client.close()
        await server.stop()

    restarted = RedisServer(config)
    await restarted.start()
    check = RespClient(config.host, config.port)
    await check.connect()
    try:
        assert await check.execute("GET", "version") == b"19"
        assert await check.execute("TTL", "version") > 0
        assert await check.execute("RPOP", "items") == b"a"
        assert await check.execute("HGETALL", "meta") == [b"field", b"value"]
    finally:
        await check.close()
        await restarted.stop()


@pytest.mark.asyncio
async def test_aof_failure_returns_misconf_without_hanging_shutdown(
    tmp_path: Path,
) -> None:
    """写盘故障必须返回 MISCONF，后续写入和关闭都不能永久等待。"""
    config = Config(
        host="127.0.0.1",
        port=unused_port(),
        dir=tmp_path / "aof-failure",
        aof_enabled=True,
        aof_fsync="always",
        log_level="warning",
    )
    server = RedisServer(config)
    await server.start()

    def fail_write(payload: bytes) -> None:
        raise OSError("simulated disk full")

    server.aof._write_sync = fail_write
    client = RespClient(config.host, config.port)
    await client.connect()
    try:
        first = await client.execute("SET", "first", "1")
        assert hasattr(first, "message")
        assert "MISCONF" in first.message
        second = await client.execute("SET", "second", "2")
        assert hasattr(second, "message")
        assert "MISCONF" in second.message
        assert server.store.get_string(b"second") is None
    finally:
        await client.close()
        await asyncio.wait_for(server.stop(), timeout=2.0)


@pytest.mark.asyncio
async def test_aof_failure_completes_all_queued_waiters(tmp_path: Path) -> None:
    """AOF 写入失败时，所有已排队请求都必须收到异常，不能永久等待。"""
    path = tmp_path / "queued-failure.aof"
    log = AOFLog(
        path,
        fsync_policy="always",
        queue_max_commands=8,
        enqueue_timeout_sec=1.0,
    )
    log.open()
    await log.start_background_fsync()

    def fail_write(payload: bytes) -> None:
        raise OSError("simulated disk full")

    log._write_sync = fail_write
    try:
        results = await asyncio.wait_for(
            asyncio.gather(
                *(log.append([b"SET", str(i).encode(), b"v"]) for i in range(4)),
                return_exceptions=True,
            ),
            timeout=2.0,
        )
        assert len(results) == 4
        assert all(isinstance(result, OSError) for result in results)
    finally:
        await log.close()
