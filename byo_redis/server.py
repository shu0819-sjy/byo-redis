"""asyncio Redis server: accept connections, dispatch commands, persistence hooks."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

from byo_redis import __version__
from byo_redis.commands.base import WRITE_COMMANDS, CommandContext, RespError
from byo_redis.commands.registry import CommandRegistry, create_default_registry
from byo_redis.config import Config
from byo_redis.persistence.aof import AOFLog, replay_aof
from byo_redis.persistence.rdb import dump_rdb, dump_rdb_bytes, load_rdb_into
from byo_redis.protocol.encoder import encode_error, encode_value
from byo_redis.protocol.parser import ProtocolError, RespParser
from byo_redis.replication.master import MasterReplication
from byo_redis.replication.replica import ReplicaClient
from byo_redis.storage.store import Store

logger = logging.getLogger(__name__)

# Sentinel distinct from Redis null bulk (Python None) for handlers with no client reply.
_NO_CLIENT_REPLY = object()

# Defaults when Config has no dedicated fields (kept configurable on the parser).
DEFAULT_MAX_BUFFER_BYTES = 32_000_000
DEFAULT_PARTIAL_TIMEOUT_SEC = 30.0


class RedisServer:
    def __init__(self, config: Config, registry: CommandRegistry | None = None) -> None:
        self.config = config
        self.store = Store()
        self.registry = registry or create_default_registry()
        self.version = __version__
        self.master = MasterReplication(store=self.store)
        self.aof = AOFLog(
            config.aof_path,
            fsync_policy=config.aof_fsync,
            enabled=config.aof_enabled,
        )
        self._server: asyncio.Server | None = None
        self._conn_seq = 0
        self._bg_tasks: list[asyncio.Task[Any]] = []
        self._replica: ReplicaClient | None = None
        self.last_save_time: int = 0
        self._bgsave_lock = asyncio.Lock()
        self._bgsave_task: asyncio.Task[None] | None = None
        self._started = False
        self._writers: dict[int, asyncio.StreamWriter] = {}
        # Configurable buffer limit for RESP parser (bytes)
        self.max_buffer_bytes: int = int(
            getattr(config, "max_buffer_bytes", DEFAULT_MAX_BUFFER_BYTES)
        )
        self.partial_timeout_sec: float = float(
            getattr(config, "partial_timeout_sec", DEFAULT_PARTIAL_TIMEOUT_SEC)
        )

    @property
    def connected_replicas(self) -> int:
        return self.master.connected_replicas

    @property
    def replica_link_status(self) -> str:
        if self._replica is None:
            return "down"
        return self._replica.link_status

    @property
    def bgsave_in_progress(self) -> bool:
        return self._bgsave_task is not None and not self._bgsave_task.done()

    async def start(self) -> None:
        if self._started:
            return
        self.config.ensure_data_dir()
        await self._load_persistence()
        self.aof.open()
        await self.aof.start_background_fsync()

        self._server = await asyncio.start_server(
            self.handle_client,
            host=self.config.host,
            port=self.config.port,
        )
        sockets = self._server.sockets or []
        addrs = ", ".join(str(s.getsockname()) for s in sockets)
        logger.info(
            "BYO-Redis %s listening on %s (role=%s)",
            self.version,
            addrs,
            self.config.role,
        )

        if self.config.is_replica:
            assert self.config.replicaof_host and self.config.replicaof_port
            self._replica = ReplicaClient(
                self.config.replicaof_host,
                self.config.replicaof_port,
                self.store,
                self._apply_replicated_command,
                listening_port=self.config.port,
                max_bulk_len=self.config.proto_max_bulk_len,
            )
            self._bg_tasks.append(self._replica.start())

        self._bg_tasks.append(
            asyncio.create_task(self._active_expire_loop(), name="active-expire")
        )
        if self.config.rdb_save_seconds > 0:
            self._bg_tasks.append(
                asyncio.create_task(self._periodic_save_loop(), name="periodic-save")
            )
        self._started = True

    async def serve_forever(self) -> None:
        await self.start()
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        logger.info("Shutting down BYO-Redis...")
        if self._replica is not None:
            await self._replica.stop()
            self._replica = None
        if self._bgsave_task is not None and not self._bgsave_task.done():
            self._bgsave_task.cancel()
            try:
                await self._bgsave_task
            except asyncio.CancelledError:
                pass
        for task in self._bg_tasks:
            task.cancel()
        for task in self._bg_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._bg_tasks.clear()

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        await self.aof.close()
        self._started = False
        logger.info("BYO-Redis stopped")

    async def _load_persistence(self) -> None:
        cfg = self.config
        if cfg.aof_enabled and cfg.aof_path.exists() and cfg.aof_path.stat().st_size > 0:
            logger.info("Loading AOF from %s", cfg.aof_path)

            async def apply(argv: list[bytes]) -> None:
                await self._execute(argv, is_loading=True, is_replica_client=True)

            await replay_aof(
                cfg.aof_path, apply, max_bulk_len=cfg.proto_max_bulk_len
            )
            return
        if cfg.rdb_path.exists() and cfg.rdb_path.stat().st_size > 0:
            load_rdb_into(self.store, cfg.rdb_path)
            return
        logger.info("No persistence files found; starting with empty DB")

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._conn_seq += 1
        conn_id = self._conn_seq
        self._writers[conn_id] = writer
        peer = writer.get_extra_info("peername")
        logger.debug("Client %d connected from %s", conn_id, peer)
        parser = RespParser(
            max_bulk_len=self.config.proto_max_bulk_len,
            max_buffer_bytes=self.max_buffer_bytes,
        )
        became_replica = False
        partial_since: float | None = None
        try:
            while True:
                timeout = None
                if parser.buffered_size > 0 and partial_since is not None:
                    remaining = self.partial_timeout_sec - (
                        time.monotonic() - partial_since
                    )
                    if remaining <= 0:
                        raise ProtocolError(
                            f"incomplete RESP frame timed out after "
                            f"{self.partial_timeout_sec:.0f}s"
                        )
                    timeout = remaining
                try:
                    if timeout is None:
                        data = await reader.read(65536)
                    else:
                        data = await asyncio.wait_for(
                            reader.read(65536), timeout=timeout
                        )
                except asyncio.TimeoutError as exc:
                    raise ProtocolError(
                        f"incomplete RESP frame timed out after "
                        f"{self.partial_timeout_sec:.0f}s"
                    ) from exc
                if not data:
                    break
                try:
                    messages = parser.feed(data)
                except ProtocolError as exc:
                    logger.warning("Protocol error on conn %d: %s", conn_id, exc)
                    try:
                        writer.write(encode_error(f"ERR protocol error: {exc}"))
                        await writer.drain()
                    except Exception:
                        pass
                    break
                if parser.buffered_size > 0:
                    if partial_since is None:
                        partial_since = time.monotonic()
                else:
                    partial_since = None
                for msg in messages:
                    try:
                        argv = self._normalize_argv(msg)
                    except ProtocolError as exc:
                        writer.write(encode_error(f"ERR protocol error: {exc}"))
                        await writer.drain()
                        continue
                    if not argv:
                        continue

                    cmd = argv[0].upper()
                    if cmd == b"PSYNC" and not self.config.is_replica:
                        try:
                            await self.master.full_resync(writer, conn_id)
                            became_replica = True
                        except Exception:
                            logger.exception("PSYNC failed for conn %d", conn_id)
                            # Ensure unregister if register partially succeeded
                            if conn_id in self.master.replicas:
                                await self.master.unregister(conn_id)
                            try:
                                writer.write(encode_error("ERR PSYNC failed"))
                                await writer.drain()
                            except Exception:
                                pass
                            return
                        await self._hold_replica_connection(reader, conn_id)
                        return

                    reply = await self._execute(argv, connection_id=conn_id)
                    if reply is not None:
                        writer.write(reply)
                        await writer.drain()
        except ProtocolError as exc:
            logger.warning("Protocol error on conn %d: %s", conn_id, exc)
            try:
                writer.write(encode_error(f"ERR protocol error: {exc}"))
                await writer.drain()
            except Exception:
                pass
        except (ConnectionError, OSError) as exc:
            logger.debug("Client %d disconnected: %s", conn_id, exc)
        finally:
            # Unregister whenever this conn is (still) a tracked replica
            if became_replica or conn_id in self.master.replicas:
                await self.master.unregister(conn_id)
            self._writers.pop(conn_id, None)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            logger.debug("Client %d closed", conn_id)

    async def _hold_replica_connection(
        self, reader: asyncio.StreamReader, conn_id: int
    ) -> None:
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                logger.debug(
                    "Ignoring %d bytes from replica conn %d", len(data), conn_id
                )
        except (ConnectionError, OSError):
            pass

    def _normalize_argv(self, msg: Any) -> list[bytes]:
        if not isinstance(msg, list):
            raise ProtocolError("expected array command")
        argv: list[bytes] = []
        for item in msg:
            if isinstance(item, (bytes, bytearray)):
                argv.append(bytes(item))
            elif isinstance(item, str):
                argv.append(item.encode("utf-8"))
            elif item is None:
                argv.append(b"")
            else:
                raise ProtocolError(
                    f"command arg must be bulk string, got {type(item)}"
                )
        return argv

    async def _execute(
        self,
        argv: list[bytes],
        *,
        connection_id: int = 0,
        is_replica_client: bool = False,
        is_loading: bool = False,
        is_replica_link: bool = False,
    ) -> bytes | None:
        ctx = CommandContext(
            store=self.store,
            config=self.config,
            server=self,
            argv=argv,
            is_replica_client=is_replica_client,
            is_loading=is_loading,
            is_replica_link=is_replica_link,
            connection_id=connection_id,
        )
        try:
            result = await self.registry.dispatch(ctx)
        except RespError as exc:
            return exc.to_resp()
        except Exception:
            logger.exception("Internal error handling %r", argv[0] if argv else None)
            return encode_error("ERR internal error")

        if result is _NO_CLIENT_REPLY:
            return None

        upper = argv[0].upper()
        if (
            upper in WRITE_COMMANDS
            and not is_loading
            and not is_replica_client
            and not self.config.is_replica
        ):
            try:
                self.aof.append(argv)
            except OSError:
                logger.exception("AOF append failed")
            # Non-blocking: must not await per-replica network completion
            self.master.propagate(argv)
        elif upper in WRITE_COMMANDS and is_replica_client and not is_loading:
            try:
                self.aof.append(argv)
            except OSError:
                logger.exception("AOF append failed on replica")

        try:
            return encode_value(result)
        except TypeError:
            logger.exception("Failed to encode result for %r", argv[0])
            return encode_error("ERR internal error")

    async def _apply_replicated_command(self, argv: list[bytes]) -> None:
        await self._execute(argv, is_replica_client=True)

    def save_rdb_sync(self) -> None:
        dump_rdb(self.store, self.config.rdb_path)
        self.last_save_time = int(time.time())

    def schedule_bgsave(self) -> bool:
        """Start a background BGSAVE task. Returns False if already in progress."""
        if self.bgsave_in_progress:
            return False
        self._bgsave_task = asyncio.create_task(
            self._bgsave_worker(), name="bgsave"
        )
        return True

    async def _bgsave_worker(self) -> None:
        async with self._bgsave_lock:
            try:
                payload = dump_rdb_bytes(self.store)
                path = self.config.rdb_path

                def _write() -> None:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    tmp = path.with_suffix(path.suffix + ".tmp")
                    with open(tmp, "wb") as fh:
                        fh.write(payload)
                        fh.flush()
                        os.fsync(fh.fileno())
                    os.replace(tmp, path)

                await asyncio.to_thread(_write)
                self.last_save_time = int(time.time())
                logger.info("BGSAVE completed (%d bytes)", len(payload))
            except Exception:
                logger.exception("BGSAVE failed")

    async def bgsave(self) -> None:
        """Await a full background save (used by periodic saver)."""
        if not self.schedule_bgsave():
            # Already running — wait for the in-flight task
            if self._bgsave_task is not None:
                await self._bgsave_task
            return
        assert self._bgsave_task is not None
        await self._bgsave_task

    async def handle_psync(self, ctx: CommandContext) -> object:
        writer = self._writers.get(ctx.connection_id)
        if writer is None:
            raise RespError("ERR PSYNC requires active connection")
        await self.master.full_resync(writer, ctx.connection_id)
        return _NO_CLIENT_REPLY

    async def _active_expire_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(0.1)
                self.store.active_expire_cycle(sample_size=20)
        except asyncio.CancelledError:
            return

    async def _periodic_save_loop(self) -> None:
        interval = self.config.rdb_save_seconds
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    await self.bgsave()
                except Exception:
                    logger.exception("Periodic SAVE failed")
        except asyncio.CancelledError:
            return
