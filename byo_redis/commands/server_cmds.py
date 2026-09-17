"""Server/admin commands: INFO, SAVE, BGSAVE, CONFIG, DEL, DBSIZE."""

from __future__ import annotations

import logging

from .base import CommandContext, RespError, wrong_arity
from .connection_cmds import _Simple

logger = logging.getLogger(__name__)


def cmd_info(ctx: CommandContext) -> object:
    section = b"default"
    if len(ctx.argv) == 2:
        section = ctx.argv[1].lower()
    elif len(ctx.argv) > 2:
        raise wrong_arity("info")

    role = "slave" if ctx.config.is_replica else "master"
    key_count, expires, avg_ttl = ctx.store.keyspace_stats()
    lines: list[str] = [
        "# Server",
        f"redis_version:{ctx.server.version}",
        "redis_mode:standalone",
        f"tcp_port:{ctx.config.port}",
        f"uptime_in_seconds:{ctx.server.uptime_seconds}",
        f"total_connections_received:{ctx.server.total_connections_received}",
        f"rejected_connections:{ctx.server.rejected_connections}",
        f"total_commands_processed:{ctx.server.total_commands_processed}",
            f"total_commands_failed:{ctx.server.total_commands_failed}",
            f"used_memory:{ctx.store.memory_usage()}",
            f"maxmemory:{ctx.config.maxmemory_bytes}",
            f"maxmemory_policy:{ctx.config.maxmemory_policy}",
            f"evicted_keys:{ctx.store.evicted_keys}",
        "",
        "# Clients",
        f"connected_clients:{ctx.server.connected_clients}",
        f"maxclients:{ctx.config.max_clients}",
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
            f"db0:keys={key_count},expires={expires},avg_ttl={avg_ttl}",
            "",
            "# Persistence",
            f"aof_enabled:{1 if ctx.config.aof_enabled else 0}",
            f"aof_pending_commands:{ctx.server.aof.pending_commands}",
            f"aof_healthy:{1 if ctx.server.aof.healthy else 0}",
            f"aof_last_fsync_time:{int(ctx.server.aof.last_fsync_time)}",
            f"aof_rewrite_in_progress:{1 if ctx.server.aof_rewrite_in_progress else 0}",
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


def cmd_save(ctx: CommandContext) -> object:
    if len(ctx.argv) != 1:
        raise wrong_arity("save")
    try:
        ctx.server.save_rdb_sync()
    except OSError as exc:
        logger.exception("SAVE failed")
        raise RespError(f"ERR SAVE failed: {exc}") from exc
    return _Simple("OK")


def cmd_bgsave(ctx: CommandContext) -> object:
    """Schedule BGSAVE in the background; return immediately."""
    if len(ctx.argv) != 1:
        raise wrong_arity("bgsave")
    started = ctx.server.schedule_bgsave()
    if not started:
        raise RespError("ERR Background save already in progress")
    return _Simple("Background saving started")


def cmd_bgrewriteaof(ctx: CommandContext) -> object:
    """启动后台 AOF 压缩任务。"""
    if len(ctx.argv) != 1:
        raise wrong_arity("bgrewriteaof")
    if not ctx.config.aof_enabled:
        raise RespError("ERR AOF is disabled")
    if not ctx.server.schedule_aof_rewrite():
        raise RespError("ERR Background append only file rewriting already in progress")
    return _Simple("Background append only file rewriting started")


def cmd_config(ctx: CommandContext) -> object:
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
        raise RespError("ERR CONFIG SET not supported in BYO-Redis v0.2.2")
    raise RespError(
        f"ERR Unknown subcommand or wrong number of arguments for 'config|{sub.decode()}'"
    )


def cmd_del(ctx: CommandContext) -> object:
    if len(ctx.argv) < 2:
        raise wrong_arity("del")
    return ctx.store.delete(*ctx.argv[1:])


def cmd_dbsize(ctx: CommandContext) -> object:
    if len(ctx.argv) != 1:
        raise wrong_arity("dbsize")
    return ctx.store.dbsize()
