"""Hash command unit tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from byo_redis.commands.base import CommandContext, RespError
from byo_redis.commands.hash_cmds import cmd_hgetall, cmd_hset
from byo_redis.config import Config
from byo_redis.server import RedisServer
from byo_redis.storage.store import Store


def _ctx() -> CommandContext:
    store = Store()
    config = Config(dir=Path("."), aof_enabled=False, port=0)
    server = RedisServer(config)
    server.store = store
    return CommandContext(store=store, config=config, server=server)


def test_hset_hgetall() -> None:
    ctx = _ctx()
    ctx.argv = [b"HSET", b"h", b"f1", b"v1"]
    assert cmd_hset(ctx) == 1
    ctx.argv = [b"HSET", b"h", b"f1", b"v2"]
    assert cmd_hset(ctx) == 0
    ctx.argv = [b"HGETALL", b"h"]
    flat = cmd_hgetall(ctx)
    assert isinstance(flat, list)
    mapping = dict(zip(flat[0::2], flat[1::2], strict=True))
    assert mapping == {b"f1": b"v2"}


def test_hgetall_missing() -> None:
    ctx = _ctx()
    ctx.argv = [b"HGETALL", b"nope"]
    assert cmd_hgetall(ctx) == []


def test_wrongtype() -> None:
    ctx = _ctx()
    ctx.store.set_string(b"s", b"v")
    ctx.argv = [b"HSET", b"s", b"f", b"v"]
    with pytest.raises(RespError) as ei:
        cmd_hset(ctx)
    assert "WRONGTYPE" in ei.value.message
