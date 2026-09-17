"""Replication commands handled on the master side: REPLCONF, PSYNC."""

from __future__ import annotations

from .base import CommandContext, wrong_arity
from .connection_cmds import _Simple


def cmd_replconf(ctx: CommandContext) -> object:
    # Accept any REPLCONF ... and reply OK — enough for handshake.
    if len(ctx.argv) < 2:
        raise wrong_arity("replconf")
    return _Simple("OK")


async def cmd_psync(ctx: CommandContext) -> object:
    if len(ctx.argv) != 3:
        raise wrong_arity("psync")
    # v0.2.2 仍采用全量同步；处理器返回“不向普通客户端回复”哨兵。
    return await ctx.server.handle_psync(ctx)
