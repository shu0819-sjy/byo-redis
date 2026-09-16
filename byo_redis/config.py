"""Configuration loading and validation for BYO-Redis."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


AofFsync = Literal["always", "everysec", "no"]
Role = Literal["master", "replica"]
LogLevel = Literal["debug", "info", "warning", "error"]


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
        raise argparse.ArgumentTypeError(
            "replicaof must be HOST:PORT, e.g. 127.0.0.1:6379"
        )
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
    return parser


def config_from_args(argv: list[str] | None = None) -> Config:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if not (1 <= args.port <= 65535):
        parser.error(f"port out of range: {args.port}")
    if args.proto_max_bulk_len < 1024:
        parser.error("proto-max-bulk-len must be >= 1024")
    if args.rdb_save_seconds < 0:
        parser.error("rdb-save-seconds must be >= 0")

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
    )
