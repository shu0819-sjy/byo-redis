"""连接、协议和资源限制的回归测试。"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from byo_redis.config import Config
from byo_redis.protocol.parser import ProtocolError, ProtocolErrorMsg, RespParser
from byo_redis.server import RedisServer
from tests.conftest import RespClient, unused_port


def test_parser_rejects_excessive_array_nesting() -> None:
    """恶意嵌套数组不能触发 Python 递归溢出。"""
    parser = RespParser(max_array_depth=2)
    payload = b"*1\r\n" * 3 + b"$1\r\nx\r\n"
    with pytest.raises(ProtocolError, match="array nesting"):
        parser.feed(payload)


@pytest.mark.asyncio
async def test_server_applies_array_nesting_limit(tmp_path: Path) -> None:
    """服务器必须把配置的数组嵌套上限传给连接解析器。"""
    config = Config(
        host="127.0.0.1",
        port=unused_port(),
        dir=tmp_path / "array-depth",
        aof_enabled=False,
        max_array_depth=1,
        log_level="warning",
    )
    server = RedisServer(config)
    await server.start()
    reader, writer = await asyncio.open_connection(config.host, config.port)
    try:
        writer.write(b"*1\r\n*1\r\n$4\r\nPING\r\n")
        await writer.drain()
        response = await asyncio.wait_for(reader.readuntil(b"\r\n"), timeout=1.0)
        assert b"array nesting" in response
    finally:
        writer.close()
        await writer.wait_closed()
        await server.stop()


@pytest.mark.asyncio
async def test_server_rejects_connections_over_limit(tmp_path: Path) -> None:
    """达到 max_clients 后，新连接收到明确错误并被关闭。"""
    config = Config(
        host="127.0.0.1",
        port=unused_port(),
        dir=tmp_path / "clients",
        aof_enabled=False,
        max_clients=1,
        log_level="warning",
    )
    server = RedisServer(config)
    await server.start()
    first = RespClient(config.host, config.port)
    second = RespClient(config.host, config.port)
    await first.connect()
    await second.connect()
    try:
        assert await first.execute("PING") == "PONG"
        result = await second.execute("PING")
        assert isinstance(result, ProtocolErrorMsg)
        assert "max number of clients" in result.message
        await asyncio.sleep(0)
        assert server._client_count == 1
    finally:
        await first.close()
        await second.close()
        await server.stop()


@pytest.mark.asyncio
async def test_auth_is_required_when_password_is_configured(tmp_path: Path) -> None:
    """配置密码后，普通命令必须先通过 AUTH。"""
    config = Config(
        host="127.0.0.1",
        port=unused_port(),
        dir=tmp_path / "auth",
        aof_enabled=False,
        requirepass="secret-value",
        log_level="warning",
    )
    server = RedisServer(config)
    await server.start()
    client = RespClient(config.host, config.port)
    await client.connect()
    try:
        result = await client.execute("PING")
        assert isinstance(result, ProtocolErrorMsg)
        assert "NOAUTH" in result.message
        wrong = await client.execute("AUTH", "bad")
        assert isinstance(wrong, ProtocolErrorMsg)
        assert "WRONGPASS" in wrong.message
        assert await client.execute("AUTH", "secret-value") == "OK"
        assert await client.execute("PING") == "PONG"
    finally:
        await client.close()
        await server.stop()


@pytest.mark.asyncio
async def test_unprotected_non_loopback_bind_is_rejected(tmp_path: Path) -> None:
    """无认证时默认拒绝监听所有网卡。"""
    config = Config(
        host="0.0.0.0",
        port=unused_port(),
        dir=tmp_path / "unsafe-bind",
        aof_enabled=False,
        log_level="warning",
    )
    server = RedisServer(config)
    with pytest.raises(ValueError, match="non-loopback"):
        await server.start()


@pytest.mark.asyncio
async def test_idle_connection_is_closed(tmp_path: Path) -> None:
    """空闲连接不能永久占用 max_clients 配额。"""
    config = Config(
        host="127.0.0.1",
        port=unused_port(),
        dir=tmp_path / "idle",
        aof_enabled=False,
        client_idle_timeout_sec=0.05,
        log_level="warning",
    )
    server = RedisServer(config)
    await server.start()
    reader, writer = await asyncio.open_connection(config.host, config.port)
    try:
        assert await asyncio.wait_for(reader.read(1), timeout=1.0) == b""
    finally:
        writer.close()
        await writer.wait_closed()
        await server.stop()


@pytest.mark.asyncio
async def test_repeated_auth_failures_close_connection(tmp_path: Path) -> None:
    """单连接密码爆破达到阈值后由服务端主动关闭。"""
    config = Config(
        host="127.0.0.1",
        port=unused_port(),
        dir=tmp_path / "auth-limit",
        aof_enabled=False,
        requirepass="correct",
        max_auth_failures=2,
        log_level="warning",
    )
    server = RedisServer(config)
    await server.start()
    client = RespClient(config.host, config.port)
    await client.connect()
    try:
        for _ in range(2):
            result = await client.execute("AUTH", "wrong")
            assert isinstance(result, ProtocolErrorMsg)
        assert client._reader is not None
        assert await asyncio.wait_for(client._reader.read(1), timeout=1.0) == b""
    finally:
        await client.close()
        await server.stop()


@pytest.mark.asyncio
async def test_persistence_filenames_cannot_escape_data_dir(tmp_path: Path) -> None:
    """RDB/AOF 文件参数只能是普通文件名，不能包含目录穿越。"""
    config = Config(
        host="127.0.0.1",
        port=unused_port(),
        dir=tmp_path / "data",
        dbfilename="../outside.rdb",
        aof_enabled=False,
        log_level="warning",
    )
    server = RedisServer(config)
    with pytest.raises(ValueError, match="plain filename"):
        await server.start()
