"""Integration: connectivity, PING, pipeline, unknown command, core types."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from byo_redis.config import Config
from byo_redis.protocol.encoder import encode_command
from byo_redis.protocol.parser import ProtocolErrorMsg
from byo_redis.server import RedisServer
from tests.conftest import RespClient, unused_port


@pytest.mark.asyncio
async def test_ping(client: RespClient) -> None:
    assert await client.execute("PING") == "PONG"
    assert await client.execute("PING", "hello") == b"hello"


@pytest.mark.asyncio
async def test_echo_and_unknown(client: RespClient) -> None:
    assert await client.execute("ECHO", "hi") == b"hi"
    err = await client.execute("NOSUCH")
    assert isinstance(err, ProtocolErrorMsg)
    assert "unknown command" in err.message


@pytest.mark.asyncio
async def test_pipeline_sticky(client: RespClient) -> None:
    payload = encode_command([b"PING"]) + encode_command([b"ECHO", b"z"])
    # send both, read two replies
    assert client._writer is not None and client._reader is not None
    client._writer.write(payload)
    await client._writer.drain()
    replies = []
    while len(replies) < 2:
        data = await client._reader.read(65536)
        assert data
        replies.extend(client._parser.feed(data))
    assert replies[0] == "PONG"
    assert replies[1] == b"z"


@pytest.mark.asyncio
async def test_string_expire(client: RespClient) -> None:
    assert await client.execute("SET", "k", "v") == "OK"
    assert await client.execute("GET", "k") == b"v"
    assert await client.execute("SET", "e", "x", "EX", "1") == "OK"
    await asyncio.sleep(1.1)
    assert await client.execute("GET", "e") is None


@pytest.mark.asyncio
async def test_list_and_hash(client: RespClient) -> None:
    assert await client.execute("LPUSH", "L", "a") == 1
    assert await client.execute("LPUSH", "L", "b") == 2
    assert await client.execute("RPOP", "L") == b"a"
    assert await client.execute("HSET", "H", "f", "1") == 1
    flat = await client.execute("HGETALL", "H")
    assert isinstance(flat, list)
    mapping = dict(zip(flat[0::2], flat[1::2], strict=True))
    assert mapping == {b"f": b"1"}


@pytest.mark.asyncio
async def test_wrongtype(client: RespClient) -> None:
    await client.execute("LPUSH", "L", "a")
    err = await client.execute("GET", "L")
    assert isinstance(err, ProtocolErrorMsg)
    assert "WRONGTYPE" in err.message


@pytest.mark.asyncio
async def test_info_reports_live_resource_and_keyspace_metrics(client: RespClient) -> None:
    """INFO 应报告真实客户端、TTL 和 AOF 健康状态。"""
    assert await client.execute("SET", "ttl-metric", "v", "EX", "60") == "OK"
    info = await client.execute("INFO")
    assert isinstance(info, bytes)
    assert b"connected_clients:1" in info
    assert b"maxclients:1000" in info
    assert b"total_connections_received:" in info
    assert b"total_commands_processed:" in info
    assert b"total_commands_failed:0" in info
    assert b"db0:keys=1,expires=1," in info
    assert b"aof_healthy:1" in info


@pytest.mark.asyncio
async def test_server_stop_closes_active_client(tmp_path: Path) -> None:
    """停止服务时，已连接客户端应观察到 EOF。"""
    config = Config(
        host="127.0.0.1",
        port=unused_port(),
        dir=tmp_path / "graceful-stop",
        aof_enabled=False,
        log_level="warning",
    )
    server = RedisServer(config)
    await server.start()
    reader, writer = await asyncio.open_connection(config.host, config.port)
    await server.stop()
    try:
        assert await asyncio.wait_for(reader.read(1), timeout=1.0) == b""
        assert server.connected_clients == 0
        assert not server._client_tasks
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_maxmemory_noeviction_returns_oom_without_dirty_write(tmp_path: Path) -> None:
    """noeviction 超限应返回 OOM，失败写入不能残留在内存中。"""
    config = Config(
        host="127.0.0.1",
        port=unused_port(),
        dir=tmp_path / "maxmemory-noeviction",
        aof_enabled=False,
        maxmemory_bytes=1,
        maxmemory_policy="noeviction",
        log_level="warning",
    )
    server = RedisServer(config)
    await server.start()
    client = RespClient(config.host, config.port)
    await client.connect()
    try:
        result = await client.execute("SET", "key", "value")
        assert isinstance(result, ProtocolErrorMsg)
        assert "OOM" in result.message
        assert server.store.get_string(b"key") is None
    finally:
        await client.close()
        await server.stop()


@pytest.mark.asyncio
async def test_maxmemory_lru_evicts_and_reports_info(tmp_path: Path) -> None:
    """allkeys-lru 应淘汰旧键并在 INFO 中报告淘汰计数。"""
    config = Config(
        host="127.0.0.1",
        port=unused_port(),
        dir=tmp_path / "maxmemory-lru",
        aof_enabled=False,
        maxmemory_bytes=220,
        maxmemory_policy="allkeys-lru",
        log_level="warning",
    )
    server = RedisServer(config)
    await server.start()
    client = RespClient(config.host, config.port)
    await client.connect()
    try:
        assert await client.execute("SET", "old", "x" * 20) == "OK"
        assert await client.execute("SET", "new", "y" * 20) == "OK"
        assert await client.execute("GET", "new") == b"y" * 20
        assert await client.execute("SET", "third", "z" * 20) == "OK"
        assert await client.execute("GET", "old") is None
        info = await client.execute("INFO")
        assert isinstance(info, bytes)
        assert b"maxmemory_policy:allkeys-lru" in info
        assert b"evicted_keys:1" in info
    finally:
        await client.close()
        await server.stop()
