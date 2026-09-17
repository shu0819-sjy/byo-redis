"""String command unit tests against Store + handlers."""

from __future__ import annotations

from pathlib import Path

import pytest

from byo_redis.commands.base import CommandContext, RespError
from byo_redis.commands.string_cmds import cmd_expire, cmd_get, cmd_set, cmd_ttl
from byo_redis.config import Config
from byo_redis.server import RedisServer
from byo_redis.storage.store import Store


def _ctx(store: Store | None = None) -> CommandContext:
    store = store or Store()
    config = Config(dir=Path("."), aof_enabled=False, port=0)
    server = RedisServer(config)
    server.store = store
    return CommandContext(store=store, config=config, server=server)


def test_set_get() -> None:
    ctx = _ctx()
    ctx.argv = [b"SET", b"k", b"v"]
    assert cmd_set(ctx).value == "OK"
    ctx.argv = [b"GET", b"k"]
    assert cmd_get(ctx) == b"v"


def test_get_missing() -> None:
    ctx = _ctx()
    ctx.argv = [b"GET", b"nope"]
    assert cmd_get(ctx) is None


def test_wrongtype_get() -> None:
    ctx = _ctx()
    ctx.store.lpush(b"L", [b"x"])
    ctx.argv = [b"GET", b"L"]
    with pytest.raises(RespError) as ei:
        cmd_get(ctx)
    assert "WRONGTYPE" in ei.value.message


def test_expire_missing() -> None:
    ctx = _ctx()
    ctx.argv = [b"EXPIRE", b"nope", b"10"]
    assert cmd_expire(ctx) == 0


def test_ttl_no_expire() -> None:
    ctx = _ctx()
    ctx.store.set_string(b"k", b"v")
    ctx.argv = [b"TTL", b"k"]
    assert cmd_ttl(ctx) == -1
