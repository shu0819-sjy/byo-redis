"""Command name → handler registry."""

from __future__ import annotations

import inspect
from typing import Any

from . import (
    connection_cmds,
    hash_cmds,
    list_cmds,
    repl_cmds,
    server_cmds,
    string_cmds,
)
from .base import (
    WRITE_COMMANDS,
    CommandContext,
    CommandHandler,
    RespError,
    readonly_error,
    unknown_command,
)


class CommandRegistry:
    def __init__(self) -> None:
        self._handlers: dict[bytes, CommandHandler] = {}

    def register(self, name: str, handler: CommandHandler) -> None:
        self._handlers[name.upper().encode("ascii")] = handler

    def get(self, name: bytes) -> CommandHandler | None:
        return self._handlers.get(name.upper())

    async def dispatch(self, ctx: CommandContext) -> Any:
        if not ctx.argv:
            raise RespError("ERR empty command")
        name = ctx.argv[0]
        handler = self.get(name)
        if handler is None:
            raise unknown_command(name)

        upper = name.upper()
        if (
            ctx.config.is_replica
            and upper in WRITE_COMMANDS
            and not ctx.is_replica_client
            and not ctx.is_loading
        ):
            raise readonly_error()

        result = handler(ctx)
        if inspect.isawaitable(result):
            return await result
        return result

    @property
    def names(self) -> list[bytes]:
        return sorted(self._handlers.keys())


def create_default_registry() -> CommandRegistry:
    reg = CommandRegistry()
    reg.register("PING", connection_cmds.cmd_ping)
    reg.register("ECHO", connection_cmds.cmd_echo)
    reg.register("SELECT", connection_cmds.cmd_select)
    reg.register("COMMAND", connection_cmds.cmd_command)

    reg.register("SET", string_cmds.cmd_set)
    reg.register("GET", string_cmds.cmd_get)
    reg.register("EXPIRE", string_cmds.cmd_expire)
    reg.register("TTL", string_cmds.cmd_ttl)

    reg.register("LPUSH", list_cmds.cmd_lpush)
    reg.register("RPOP", list_cmds.cmd_rpop)

    reg.register("HSET", hash_cmds.cmd_hset)
    reg.register("HGETALL", hash_cmds.cmd_hgetall)

    reg.register("INFO", server_cmds.cmd_info)
    reg.register("SAVE", server_cmds.cmd_save)
    reg.register("BGSAVE", server_cmds.cmd_bgsave)
    reg.register("CONFIG", server_cmds.cmd_config)
    reg.register("DEL", server_cmds.cmd_del)
    reg.register("DBSIZE", server_cmds.cmd_dbsize)

    reg.register("REPLCONF", repl_cmds.cmd_replconf)
    reg.register("PSYNC", repl_cmds.cmd_psync)
    return reg
