"""Hash commands: HSET, HGETALL."""

from __future__ import annotations

from typing import Any

from byo_redis.storage.store import WrongTypeError

from .base import CommandContext, RespError, wrong_arity, wrongtype


def cmd_hset(ctx: CommandContext) -> Any:
    # HSET key field value [field value ...]
    argc = len(ctx.argv)
    if argc < 4 or (argc % 2) != 0:
        # key + pairs → total argc must be even and >= 4
        raise wrong_arity("hset")
    key = ctx.argv[1]
    pairs = ctx.argv[2:]
    if len(pairs) % 2 != 0:
        raise RespError("ERR wrong number of arguments for 'hset' command")
    items: list[tuple[bytes, bytes]] = []
    for i in range(0, len(pairs), 2):
        items.append((pairs[i], pairs[i + 1]))
    try:
        return ctx.store.hset(key, items)
    except WrongTypeError as exc:
        raise wrongtype() from exc


def cmd_hgetall(ctx: CommandContext) -> Any:
    if len(ctx.argv) != 2:
        raise wrong_arity("hgetall")
    try:
        return ctx.store.hgetall(ctx.argv[1])
    except WrongTypeError as exc:
        raise wrongtype() from exc
