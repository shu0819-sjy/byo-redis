"""Connection-oriented commands: PING, ECHO, SELECT, COMMAND."""

from __future__ import annotations

from typing import Any

from .base import CommandContext, RespError, wrong_arity


def cmd_ping(ctx: CommandContext) -> Any:
    argc = len(ctx.argv)
    if argc == 1:
        return _Simple("PONG")
    if argc == 2:
        return ctx.argv[1]
    raise wrong_arity("ping")


def cmd_echo(ctx: CommandContext) -> Any:
    if len(ctx.argv) != 2:
        raise wrong_arity("echo")
    return ctx.argv[1]


def cmd_select(ctx: CommandContext) -> Any:
    if len(ctx.argv) != 2:
        raise wrong_arity("select")
    try:
        index = int(ctx.argv[1])
    except ValueError as exc:
        raise RespError("ERR invalid DB index") from exc
    if index != 0:
        raise RespError("ERR DB index is out of range")
    return _Simple("OK")


def cmd_command(ctx: CommandContext) -> Any:
    # Minimal stub so redis-cli handshake does not fail.
    return []


class _Simple:
    """Marker for RESP simple string encoding."""

    __slots__ = ("value",)

    def __init__(self, value: str) -> None:
        self.value = value

    def to_resp(self) -> bytes:
        from byo_redis.protocol.encoder import encode_simple_string

        return encode_simple_string(self.value)
