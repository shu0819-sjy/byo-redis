"""Shared fixtures for BYO-Redis tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest_asyncio

from byo_redis.config import Config
from byo_redis.protocol.encoder import encode_command
from byo_redis.protocol.parser import RespParser
from byo_redis.server import RedisServer


class RespClient:
    """Minimal async RESP client for integration tests."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._parser = RespParser()

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)

    async def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
        self._reader = None
        self._writer = None

    async def execute(self, *parts: str | bytes) -> object:
        argv = [
            p.encode("utf-8") if isinstance(p, str) else p for p in parts
        ]
        assert self._writer is not None and self._reader is not None
        self._writer.write(encode_command(argv))
        await self._writer.drain()
        while True:
            if self._parser.buffered_size:
                msgs = self._parser.feed(b"")
                if msgs:
                    return msgs[0]
            data = await self._reader.read(65536)
            if not data:
                raise ConnectionError("server closed")
            msgs = self._parser.feed(data)
            if msgs:
                return msgs[0]

    async def execute_raw(self, payload: bytes) -> list[object]:
        assert self._writer is not None and self._reader is not None
        self._writer.write(payload)
        await self._writer.drain()
        collected: list[object] = []
        # Read until at least one message; for pipelines caller knows count
        data = await self._reader.read(65536)
        if not data:
            raise ConnectionError("server closed")
        collected.extend(self._parser.feed(data))
        return collected


def unused_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest_asyncio.fixture
async def redis_server(tmp_path: Path) -> AsyncIterator[tuple[RedisServer, Config]]:
    port = unused_port()
    config = Config(
        host="127.0.0.1",
        port=port,
        dir=tmp_path / "data",
        aof_enabled=True,
        aof_fsync="always",
        log_level="warning",
    )
    server = RedisServer(config)
    await server.start()
    try:
        yield server, config
    finally:
        await server.stop()


@pytest_asyncio.fixture
async def client(redis_server: tuple[RedisServer, Config]) -> AsyncIterator[RespClient]:
    server, config = redis_server
    c = RespClient(config.host, config.port)
    await c.connect()
    try:
        yield c
    finally:
        await c.close()
