"""List command unit tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from byo_redis.commands.base import CommandContext, RespError
from byo_redis.commands.list_cmds import cmd_lpush, cmd_rpop
from byo_redis.config import Config
from byo_redis.server import RedisServer
from byo_redis.storage.store import Store


def _ctx() -> CommandContext:
    store = Store()
    config = Config(dir=Path("."), aof_enabled=False, port=0)
    server = RedisServer(config)
    server.store = store
    return CommandContext(store=store, config=config, server=server)


def test_lpush_rpop_order() -> None:
    ctx = _ctx()
    ctx.argv = [b"LPUSH", b"mylist", b"a"]
    assert cmd_lpush(ctx) == 1
    ctx.argv = [b"LPUSH", b"mylist", b"b"]
    assert cmd_lpush(ctx) == 2
    ctx.argv = [b"RPOP", b"mylist"]
    assert cmd_rpop(ctx) == b"a"
    ctx.argv = [b"RPOP", b"mylist"]
    assert cmd_rpop(ctx) == b"b"
    ctx.argv = [b"RPOP", b"mylist"]
    assert cmd_rpop(ctx) is None


def test_lpush_multi() -> None:
    ctx = _ctx()
    ctx.argv = [b"LPUSH", b"mylist", b"x", b"y"]
    assert cmd_lpush(ctx) == 2
    # Final left is last arg y; RPOP pops right → x first
    ctx.argv = [b"RPOP", b"mylist"]
    assert cmd_rpop(ctx) == b"x"
    ctx.argv = [b"RPOP", b"mylist"]
    assert cmd_rpop(ctx) == b"y"


def test_wrongtype() -> None:
    ctx = _ctx()
    ctx.store.set_string(b"s", b"v")
    ctx.argv = [b"LPUSH", b"s", b"x"]
    with pytest.raises(RespError) as ei:
        cmd_lpush(ctx)
    assert "WRONGTYPE" in ei.value.message
