"""Configuration loading and validation for BYO-Redis."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

AofFsync = Literal["always", "everysec", "no"]
Role = Literal["master", "replica"]
LogLevel = Literal["debug", "info", "warning", "error"]
MaxmemoryPolicy = Literal["noeviction", "allkeys-lru", "volatile-ttl"]


@dataclass
class Config:
    """Runtime configuration for a BYO-Redis instance."""

    host: str = "127.0.0.1"
    port: int = 6379
    dir: Path = field(default_factory=lambda: Path("./data"))
    dbfilename: str = "dump.rdb"
    aof_enabled: bool = True
    aof_filename: str = "appendonly.aof"
    aof_fsync: AofFsync = "everysec"
    role: Role = "master"
    replicaof_host: str | None = None
    replicaof_port: int | None = None
    log_level: LogLevel = "info"
    rdb_save_seconds: int = 0
    proto_max_bulk_len: int = 16_777_216
    proto_max_array_len: int = 1_024
    max_array_depth: int = 16
    max_buffer_bytes: int = 32_000_000
    partial_timeout_sec: float = 30.0
    client_idle_timeout_sec: float = 300.0
    max_clients: int = 1_000
    max_auth_failures: int = 5
    aof_queue_max_commands: int = 10_000
    aof_enqueue_timeout_sec: float = 5.0
    replica_backlog_max_bytes: int = 64 * 1024 * 1024
    replica_pending_max_commands: int = 10_000
    persistence_max_load_bytes: int = 1024 * 1024 * 1024
    maxmemory_bytes: int = 0
    maxmemory_policy: MaxmemoryPolicy = "noeviction"
    requirepass: str | None = field(default=None, repr=False)
    masterauth: str | None = field(default=None, repr=False)
    allow_unprotected_non_loopback: bool = False

    @property
    def rdb_path(self) -> Path:
        return self.dir / self.dbfilename

    @property
    def aof_path(self) -> Path:
        return self.dir / self.aof_filename

    @property
    def is_replica(self) -> bool:
        return self.role == "replica"

    def ensure_data_dir(self) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)


def _parse_replicaof(value: str) -> tuple[str, int]:
    if ":" not in value:
        raise argparse.ArgumentTypeError("replicaof must be HOST:PORT, e.g. 127.0.0.1:6379")
    host, _, port_s = value.rpartition(":")
    if not host:
        raise argparse.ArgumentTypeError("replicaof host must not be empty")
    try:
        port = int(port_s)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("replicaof port must be an integer") from exc
    if not (1 <= port <= 65535):
        raise argparse.ArgumentTypeError("replicaof port out of range")
    return host, port


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="byo-redis",
        description="BYO-Redis: Python asyncio Redis subset server",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind address")
    parser.add_argument("--port", type=int, default=6379, help="Listen port")
    parser.add_argument("--dir", type=Path, default=Path("./data"), help="Data directory")
    parser.add_argument("--dbfilename", default="dump.rdb", help="RDB filename")
    parser.add_argument(
        "--aof",
        dest="aof_enabled",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable AOF persistence (default: enabled)",
    )
    parser.add_argument("--aof-filename", default="appendonly.aof", help="AOF filename")
    parser.add_argument(
        "--aof-fsync",
        choices=["always", "everysec", "no"],
        default="everysec",
        help="AOF fsync policy",
    )
    parser.add_argument(
        "--replicaof",
        type=_parse_replicaof,
        default=None,
        help="Replicate from HOST:PORT (sets role=replica)",
    )
    parser.add_argument(
        "--log-level",
        choices=["debug", "info", "warning", "error"],
        default="info",
    )
    parser.add_argument(
        "--rdb-save-seconds",
        type=int,
        default=0,
        help="Periodic SAVE interval seconds (0=off)",
    )
    parser.add_argument(
        "--proto-max-bulk-len",
        type=int,
        default=16_777_216,
        help="Max RESP bulk string length",
    )
    parser.add_argument(
        "--proto-max-array-len",
        type=int,
        default=1_024,
        help="Max RESP array elements",
    )
    parser.add_argument(
        "--max-array-depth",
        type=int,
        default=16,
        help="Max nested RESP array depth",
    )
    parser.add_argument(
        "--max-buffer-bytes",
        type=int,
        default=32_000_000,
        help="Max buffered RESP bytes per connection",
    )
    parser.add_argument(
        "--partial-timeout-sec",
        type=float,
        default=30.0,
        help="Timeout for an incomplete RESP frame",
    )
    parser.add_argument(
        "--client-idle-timeout-sec",
        type=float,
        default=300.0,
        help="Disconnect an idle client after this many seconds",
    )
    parser.add_argument(
        "--max-clients",
        type=int,
        default=1_000,
        help="Maximum simultaneous client connections",
    )
    parser.add_argument(
        "--max-auth-failures",
        type=int,
        default=5,
        help="Close a connection after this many failed AUTH attempts",
    )
    parser.add_argument(
        "--aof-queue-max-commands",
        type=int,
        default=10_000,
        help="Maximum pending AOF commands",
    )
    parser.add_argument(
        "--aof-enqueue-timeout-sec",
        type=float,
        default=5.0,
        help="Maximum wait when the AOF queue is full",
    )
    parser.add_argument(
        "--replica-backlog-max-bytes",
        type=int,
        default=64 * 1024 * 1024,
        help="Maximum bytes buffered during replica full sync",
    )
    parser.add_argument(
        "--replica-pending-max-commands",
        type=int,
        default=10_000,
        help="Maximum pending replica drain markers",
    )
    parser.add_argument(
        "--persistence-max-load-bytes",
        type=int,
        default=1024 * 1024 * 1024,
        help="Maximum RDB/AOF bytes accepted during load or full sync",
    )
    parser.add_argument(
        "--maxmemory-bytes",
        type=int,
        default=0,
        help="Approximate in-memory keyspace limit in bytes (0=disabled)",
    )
    parser.add_argument(
        "--maxmemory-policy",
        choices=["noeviction", "allkeys-lru", "volatile-ttl"],
        default="noeviction",
        help="Eviction policy when maxmemory is reached",
    )
    parser.add_argument(
        "--requirepass",
        default=os.environ.get("BYO_REDIS_PASSWORD"),
        help="Require AUTH before commands (or set BYO_REDIS_PASSWORD)",
    )
    parser.add_argument(
        "--masterauth",
        default=os.environ.get("BYO_REDIS_MASTER_PASSWORD"),
        help="Password used by a replica to authenticate to its master",
    )
    parser.add_argument(
        "--allow-unprotected-non-loopback",
        action="store_true",
        help="Allow non-loopback bind without AUTH (unsafe)",
    )
    return parser


def config_from_args(argv: list[str] | None = None) -> Config:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if not (1 <= args.port <= 65535):
        parser.error(f"port out of range: {args.port}")
    if args.proto_max_bulk_len < 1024:
        parser.error("proto-max-bulk-len must be >= 1024")
    if args.proto_max_array_len <= 0:
        parser.error("proto-max-array-len must be > 0")
    if args.max_array_depth <= 0:
        parser.error("max-array-depth must be > 0")
    if args.rdb_save_seconds < 0:
        parser.error("rdb-save-seconds must be >= 0")
    if args.max_buffer_bytes < 1024:
        parser.error("max-buffer-bytes must be >= 1024")
    if args.partial_timeout_sec <= 0:
        parser.error("partial-timeout-sec must be > 0")
    if args.client_idle_timeout_sec <= 0:
        parser.error("client-idle-timeout-sec must be > 0")
    if args.max_clients <= 0:
        parser.error("max-clients must be > 0")
    if args.max_auth_failures <= 0:
        parser.error("max-auth-failures must be > 0")
    if args.aof_queue_max_commands <= 0:
        parser.error("aof-queue-max-commands must be > 0")
    if args.aof_enqueue_timeout_sec <= 0:
        parser.error("aof-enqueue-timeout-sec must be > 0")
    if args.replica_backlog_max_bytes < 1024:
        parser.error("replica-backlog-max-bytes must be >= 1024")
    if args.replica_pending_max_commands <= 0:
        parser.error("replica-pending-max-commands must be > 0")
    if args.persistence_max_load_bytes < 1024:
        parser.error("persistence-max-load-bytes must be >= 1024")
    if args.maxmemory_bytes < 0:
        parser.error("maxmemory-bytes must be >= 0")
    if args.requirepass == "":
        parser.error("requirepass must not be empty")
    if args.masterauth == "":
        parser.error("masterauth must not be empty")

    role: Role = "master"
    replicaof_host: str | None = None
    replicaof_port: int | None = None
    if args.replicaof is not None:
        role = "replica"
        replicaof_host, replicaof_port = args.replicaof

    return Config(
        host=args.host,
        port=args.port,
        dir=args.dir.resolve() if not args.dir.is_absolute() else args.dir,
        dbfilename=args.dbfilename,
        aof_enabled=bool(args.aof_enabled),
        aof_filename=args.aof_filename,
        aof_fsync=args.aof_fsync,
        role=role,
        replicaof_host=replicaof_host,
        replicaof_port=replicaof_port,
        log_level=args.log_level,
        rdb_save_seconds=args.rdb_save_seconds,
        proto_max_bulk_len=args.proto_max_bulk_len,
        proto_max_array_len=args.proto_max_array_len,
        max_array_depth=args.max_array_depth,
        max_buffer_bytes=args.max_buffer_bytes,
        partial_timeout_sec=args.partial_timeout_sec,
        client_idle_timeout_sec=args.client_idle_timeout_sec,
        max_clients=args.max_clients,
        max_auth_failures=args.max_auth_failures,
        aof_queue_max_commands=args.aof_queue_max_commands,
        aof_enqueue_timeout_sec=args.aof_enqueue_timeout_sec,
        replica_backlog_max_bytes=args.replica_backlog_max_bytes,
        replica_pending_max_commands=args.replica_pending_max_commands,
        persistence_max_load_bytes=args.persistence_max_load_bytes,
        maxmemory_bytes=args.maxmemory_bytes,
        maxmemory_policy=args.maxmemory_policy,
        requirepass=args.requirepass,
        masterauth=args.masterauth,
        allow_unprotected_non_loopback=args.allow_unprotected_non_loopback,
    )
