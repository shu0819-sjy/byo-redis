"""String commands: SET, GET, EXPIRE, TTL."""

from __future__ import annotations

from typing import Any

from byo_redis.storage.store import WrongTypeError

from .base import CommandContext, RespError, wrong_arity, wrongtype
from .connection_cmds import _Simple


def cmd_set(ctx: CommandContext) -> Any:
    # SET key value [EX seconds]
    argc = len(ctx.argv)
    if argc < 3:
        raise wrong_arity("set")
    key = ctx.argv[1]
    value = ctx.argv[2]
    ex_seconds: int | None = None
    i = 3
    while i < argc:
        opt = ctx.argv[i].upper()
        if opt == b"EX":
            if i + 1 >= argc:
                raise RespError("ERR syntax error")
            try:
                ex_seconds = int(ctx.argv[i + 1])
            except ValueError as exc:
                raise RespError("ERR value is not an integer or out of range") from exc
            if ex_seconds <= 0:
                raise RespError("ERR invalid expire time in set")
            i += 2
        else:
            raise RespError("ERR syntax error")
    ctx.store.set_string(key, value, ex_seconds=ex_seconds)
    return _Simple("OK")


def cmd_get(ctx: CommandContext) -> Any:
    if len(ctx.argv) != 2:
        raise wrong_arity("get")
    try:
        return ctx.store.get_string(ctx.argv[1])
    except WrongTypeError as exc:
        raise wrongtype() from exc


def cmd_expire(ctx: CommandContext) -> Any:
    if len(ctx.argv) != 3:
        raise wrong_arity("expire")
    key = ctx.argv[1]
    try:
        seconds = int(ctx.argv[2])
    except ValueError as exc:
        raise RespError("ERR value is not an integer or out of range") from exc
    return ctx.store.expire(key, seconds)


def cmd_ttl(ctx: CommandContext) -> Any:
    if len(ctx.argv) != 2:
        raise wrong_arity("ttl")
    return ctx.store.ttl(ctx.argv[1])
