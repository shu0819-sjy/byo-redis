"""Replication commands handled on the master side: REPLCONF, PSYNC."""

from __future__ import annotations

from typing import Any

from .base import CommandContext, RespError, wrong_arity
from .connection_cmds import _Simple


def cmd_replconf(ctx: CommandContext) -> Any:
    # Accept any REPLCONF ... and reply OK — enough for handshake.
    if len(ctx.argv) < 2:
        raise wrong_arity("replconf")
    return _Simple("OK")


async def cmd_psync(ctx: CommandContext) -> Any:
    if len(ctx.argv) != 3:
        raise wrong_arity("psync")
    # Full resync always in v0.1; handler returns no-client-reply sentinel
    return await ctx.server.handle_psync(ctx)
