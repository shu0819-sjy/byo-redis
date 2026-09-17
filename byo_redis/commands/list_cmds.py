"""List commands: LPUSH, RPOP."""

from __future__ import annotations

from byo_redis.storage.store import WrongTypeError

from .base import CommandContext, wrong_arity, wrongtype


def cmd_lpush(ctx: CommandContext) -> object:
    if len(ctx.argv) < 3:
        raise wrong_arity("lpush")
    key = ctx.argv[1]
    elements = ctx.argv[2:]
    try:
        return ctx.store.lpush(key, elements)
    except WrongTypeError as exc:
        raise wrongtype() from exc


def cmd_rpop(ctx: CommandContext) -> object:
    if len(ctx.argv) != 2:
        raise wrong_arity("rpop")
    try:
        return ctx.store.rpop(ctx.argv[1])
    except WrongTypeError as exc:
        raise wrongtype() from exc
