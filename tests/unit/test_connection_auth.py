"""Unit tests for connection commands and AUTH."""

from __future__ import annotations

from pathlib import Path

import pytest

from byo_redis.commands.base import CommandContext, RespError
from byo_redis.commands.connection_cmds import (
    cmd_auth,
    cmd_command,
    cmd_echo,
    cmd_ping,
    cmd_select,
)
from byo_redis.config import Config
from byo_redis.server import RedisServer
from byo_redis.storage.store import Store


def _ctx(*, requirepass: str | None = None, connection_id: int = 1) -> CommandContext:
    config = Config(dir=Path("."), aof_enabled=False, port=0, requirepass=requirepass)
    server = RedisServer(config)
    server.store = Store()
    return CommandContext(
        store=server.store,
        config=config,
        server=server,
        connection_id=connection_id,
    )


def test_ping_pong_and_echo_payload() -> None:
    from byo_redis.protocol.encoder import encode_value

    ctx = _ctx()
    ctx.argv = [b"PING"]
    pong = cmd_ping(ctx)
    assert pong.value == "PONG"
    assert encode_value(pong) == b"+PONG\r\n"
    ctx.argv = [b"PING", b"hello"]
    assert cmd_ping(ctx) == b"hello"


def test_ping_wrong_arity() -> None:
    ctx = _ctx()
    ctx.argv = [b"PING", b"a", b"b"]
    with pytest.raises(RespError, match="wrong number"):
        cmd_ping(ctx)


def test_echo_ok_and_arity() -> None:
    ctx = _ctx()
    ctx.argv = [b"ECHO", b"hi"]
    assert cmd_echo(ctx) == b"hi"
    ctx.argv = [b"ECHO"]
    with pytest.raises(RespError, match="wrong number"):
        cmd_echo(ctx)


def test_select_only_db0() -> None:
    ctx = _ctx()
    ctx.argv = [b"SELECT", b"0"]
    assert cmd_select(ctx).value == "OK"
    ctx.argv = [b"SELECT", b"1"]
    with pytest.raises(RespError, match="out of range"):
        cmd_select(ctx)
    ctx.argv = [b"SELECT", b"x"]
    with pytest.raises(RespError, match="invalid DB index"):
        cmd_select(ctx)


def test_command_stub_returns_empty_list() -> None:
    ctx = _ctx()
    ctx.argv = [b"COMMAND"]
    assert cmd_command(ctx) == []


def test_auth_without_requirepass() -> None:
    ctx = _ctx(requirepass=None)
    ctx.argv = [b"AUTH", b"secret"]
    with pytest.raises(RespError, match="without any password"):
        cmd_auth(ctx)


def test_auth_wrong_password() -> None:
    ctx = _ctx(requirepass="s3cret")
    ctx.argv = [b"AUTH", b"nope"]
    with pytest.raises(RespError, match="WRONGPASS"):
        cmd_auth(ctx)


def test_auth_success_marks_connection() -> None:
    ctx = _ctx(requirepass="s3cret", connection_id=7)
    ctx.argv = [b"AUTH", b"s3cret"]
    assert cmd_auth(ctx).value == "OK"
    assert 7 in ctx.server._authenticated_connections


def test_auth_wrong_arity() -> None:
    ctx = _ctx(requirepass="x")
    ctx.argv = [b"AUTH"]
    with pytest.raises(RespError, match="wrong number"):
        cmd_auth(ctx)


def test_authenticate_constant_time_false_without_password() -> None:
    server = RedisServer(Config(dir=Path("."), aof_enabled=False, port=0, requirepass=None))
    assert server.authenticate(1, b"anything") is False
