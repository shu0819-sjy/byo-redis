"""Integration: connectivity, PING, pipeline, unknown command, core types."""

from __future__ import annotations

import asyncio
import time

import pytest

from byo_redis.protocol.encoder import encode_command
from byo_redis.protocol.parser import ProtocolErrorMsg
from tests.conftest import RespClient


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
    mapping = dict(zip(flat[0::2], flat[1::2]))
    assert mapping == {b"f": b"1"}


@pytest.mark.asyncio
async def test_wrongtype(client: RespClient) -> None:
    await client.execute("LPUSH", "L", "a")
    err = await client.execute("GET", "L")
    assert isinstance(err, ProtocolErrorMsg)
    assert "WRONGTYPE" in err.message
