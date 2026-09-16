"""Integration: AOF append and replay on restart."""

from __future__ import annotations

from pathlib import Path

import pytest

from byo_redis.config import Config
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
        mapping = dict(zip(flat[0::2], flat[1::2]))  # type: ignore[index]
        assert mapping == {b"f": b"v"}
    finally:
        await client2.close()
        await server2.stop()
