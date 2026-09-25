"""Shared command context and RESP error helpers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from byo_redis.protocol.encoder import encode_error

if TYPE_CHECKING:
    from byo_redis.config import Config
    from byo_redis.server import RedisServer
    from byo_redis.storage.store import Store


class RespError(Exception):
    """Business-level RESP error to send to the client."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def to_resp(self) -> bytes:
        return encode_error(self.message)


def wrongtype() -> RespError:
    return RespError("WRONGTYPE Operation against a key holding the wrong kind of value")


def wrong_arity(cmd: str) -> RespError:
    return RespError(f"ERR wrong number of arguments for '{cmd}' command")


def unknown_command(name: bytes) -> RespError:
    shown = name.decode("utf-8", errors="replace")
    return RespError(f"ERR unknown command '{shown}'")


def readonly_error() -> RespError:
    return RespError("READONLY You can't write against a read only replica.")


@dataclass
class CommandContext:
    store: Store
    config: Config
    server: RedisServer
    argv: list[bytes] = field(default_factory=list)
    # True when applying commands from master replication stream or AOF replay
    is_replica_client: bool = False
    is_loading: bool = False
    # True for connections that are replica sockets attached to this master
    is_replica_link: bool = False
    connection_id: int = 0


CommandHandler = Callable[[CommandContext], object | Awaitable[object]]

# Commands that mutate data and should be AOF'd / propagated
WRITE_COMMANDS = frozenset(
    {
        b"SET",
        b"EXPIRE",
        b"LPUSH",
        b"RPOP",
        b"HSET",
        b"DEL",
    }
)
