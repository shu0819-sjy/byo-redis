"""Server/admin commands: INFO, SAVE, BGSAVE, CONFIG, DEL, DBSIZE."""

from __future__ import annotations

import logging
from typing import Any

from .base import CommandContext, RespError, wrong_arity
from .connection_cmds import _Simple

logger = logging.getLogger(__name__)


def cmd_info(ctx: CommandContext) -> Any:
    section = b"default"
    if len(ctx.argv) == 2:
        section = ctx.argv[1].lower()
    elif len(ctx.argv) > 2:
        raise wrong_arity("info")

    role = "slave" if ctx.config.is_replica else "master"
    lines: list[str] = [
        "# Server",
        f"redis_version:{ctx.server.version}",
        "redis_mode:standalone",
        f"tcp_port:{ctx.config.port}",
        "",
        "# Replication",
        f"role:{role}",
    ]
    if ctx.config.is_replica:
        lines.extend(
            [
                f"master_host:{ctx.config.replicaof_host or ''}",
                f"master_port:{ctx.config.replicaof_port or 0}",
                f"master_link_status:{ctx.server.replica_link_status}",
            ]
        )
    else:
        lines.append(f"connected_slaves:{ctx.server.connected_replicas}")

    lines.extend(
        [
            "",
            "# Keyspace",
            f"db0:keys={ctx.store.dbsize()},expires=0,avg_ttl=0",
            "",
            "# Persistence",
            f"aof_enabled:{1 if ctx.config.aof_enabled else 0}",
            f"rdb_last_save_time:{ctx.server.last_save_time}",
        ]
    )

    if section not in (
        b"default",
        b"all",
        b"server",
        b"replication",
        b"keyspace",
        b"persistence",
    ):
        # Still return full info for unknown sections — redis-cli is tolerant.
        pass

    body = "\r\n".join(lines) + "\r\n"
    return body.encode("utf-8")


def cmd_save(ctx: CommandContext) -> Any:
    if len(ctx.argv) != 1:
        raise wrong_arity("save")
    try:
        ctx.server.save_rdb_sync()
    except OSError as exc:
        logger.exception("SAVE failed")
        raise RespError(f"ERR SAVE failed: {exc}") from exc
    return _Simple("OK")


def cmd_bgsave(ctx: CommandContext) -> Any:
    """Schedule BGSAVE in the background; return immediately."""
    if len(ctx.argv) != 1:
        raise wrong_arity("bgsave")
    started = ctx.server.schedule_bgsave()
    if not started:
        raise RespError("ERR Background save already in progress")
    return _Simple("Background saving started")


def cmd_config(ctx: CommandContext) -> Any:
    if len(ctx.argv) < 2:
        raise wrong_arity("config")
    sub = ctx.argv[1].upper()
    if sub == b"GET":
        if len(ctx.argv) != 3:
            raise wrong_arity("config|get")
        pattern = ctx.argv[2].decode("utf-8", errors="replace").lower()
        pairs: list[bytes] = []
        mapping = {
            "dir": str(ctx.config.dir).encode("utf-8"),
            "dbfilename": ctx.config.dbfilename.encode("utf-8"),
        }
        for key, value in mapping.items():
            if pattern == "*" or pattern == key:
                pairs.append(key.encode("utf-8"))
                pairs.append(value)
        return pairs
    if sub == b"SET":
        raise RespError("ERR CONFIG SET not supported in BYO-Redis v0.1")
    raise RespError(
        f"ERR Unknown subcommand or wrong number of arguments for 'config|{sub.decode()}'"
    )


def cmd_del(ctx: CommandContext) -> Any:
    if len(ctx.argv) < 2:
        raise wrong_arity("del")
    return ctx.store.delete(*ctx.argv[1:])


def cmd_dbsize(ctx: CommandContext) -> Any:
    if len(ctx.argv) != 1:
        raise wrong_arity("dbsize")
    return ctx.store.dbsize()
