"""asyncio Redis server: accept connections, dispatch commands, persistence hooks."""

from __future__ import annotations

import asyncio
import hmac
import ipaddress
import logging
import os
import time

from byo_redis import __version__
from byo_redis.commands.base import WRITE_COMMANDS, CommandContext, RespError
from byo_redis.commands.registry import CommandRegistry, create_default_registry
from byo_redis.config import Config
from byo_redis.persistence.aof import AOFLog, replay_aof
from byo_redis.persistence.rdb import dump_rdb, dump_rdb_entries, load_rdb_into
from byo_redis.protocol.encoder import encode_error, encode_value
from byo_redis.protocol.parser import ProtocolError, RespParser
from byo_redis.replication.master import MasterReplication
from byo_redis.replication.replica import ReplicaClient
from byo_redis.storage.store import MemoryLimitError, Store

logger = logging.getLogger(__name__)

# Sentinel distinct from Redis null bulk (Python None) for handlers with no client reply.
_NO_CLIENT_REPLY = object()


class RedisServer:
    def __init__(self, config: Config, registry: CommandRegistry | None = None) -> None:
        self.config = config
        self.store = Store(
            maxmemory_bytes=config.maxmemory_bytes,
            maxmemory_policy=config.maxmemory_policy,
        )
        self.registry = registry or create_default_registry()
        self.version = __version__
        self._write_lock = asyncio.Lock()
        self.master = MasterReplication(
            store=self.store,
            backlog_max_bytes=config.replica_backlog_max_bytes,
            pending_max_commands=config.replica_pending_max_commands,
            snapshot_lock=self._write_lock,
        )
        self.aof = AOFLog(
            config.aof_path,
            fsync_policy=config.aof_fsync,
            enabled=config.aof_enabled,
            queue_max_commands=config.aof_queue_max_commands,
            enqueue_timeout_sec=config.aof_enqueue_timeout_sec,
        )
        self._server: asyncio.Server | None = None
        self._conn_seq = 0
        self._bg_tasks: list[asyncio.Task[None]] = []
        self._replica: ReplicaClient | None = None
        self.last_save_time: int = 0
        self._bgsave_lock = asyncio.Lock()
        self._bgsave_task: asyncio.Task[None] | None = None
        self._aof_rewrite_lock = asyncio.Lock()
        self._aof_rewrite_task: asyncio.Task[None] | None = None
        self._started = False
        self._writers: dict[int, asyncio.StreamWriter] = {}
        self._client_tasks: set[asyncio.Task[None]] = set()
        self._authenticated_connections: set[int] = set()
        self._authentication_failures: dict[int, int] = {}
        self._client_count = 0
        self._started_at = time.time()
        self._total_connections_received = 0
        self._rejected_connections = 0
        self._total_commands_processed = 0
        self._total_commands_failed = 0
        self.max_buffer_bytes = config.max_buffer_bytes
        self.partial_timeout_sec = config.partial_timeout_sec

    @property
    def connected_replicas(self) -> int:
        return self.master.connected_replicas

    @property
    def connected_clients(self) -> int:
        """返回当前占用客户端配额的连接数。"""
        return self._client_count

    @property
    def uptime_seconds(self) -> int:
        """返回服务启动后的秒数，未启动时返回零。"""
        if not self._started:
            return 0
        return max(0, int(time.time() - self._started_at))

    @property
    def total_connections_received(self) -> int:
        """返回累计接收的客户端连接数。"""
        return self._total_connections_received

    @property
    def rejected_connections(self) -> int:
        """返回因连接数上限拒绝的连接数。"""
        return self._rejected_connections

    @property
    def total_commands_processed(self) -> int:
        """返回累计处理的客户端命令数。"""
        return self._total_commands_processed

    @property
    def total_commands_failed(self) -> int:
        """返回累计返回业务或内部错误的命令数。"""
        return self._total_commands_failed

    @property
    def replica_link_status(self) -> str:
        if self._replica is None:
            return "down"
        return self._replica.link_status

    @property
    def bgsave_in_progress(self) -> bool:
        return self._bgsave_task is not None and not self._bgsave_task.done()

    @property
    def aof_rewrite_in_progress(self) -> bool:
        """返回当前是否有 AOF 重写任务。"""
        return self._aof_rewrite_task is not None and not self._aof_rewrite_task.done()

    async def start(self) -> None:
        if self._started:
            return
        self._validate_network_security()
        self._started_at = time.time()
        self.config.ensure_data_dir()
        await self._load_persistence()
        self.aof.open()
        await self.aof.start_background_fsync()

        self._server = await asyncio.start_server(
            self.handle_client,
            host=self.config.host,
            port=self.config.port,
        )
        addrs = ", ".join(
            str(server_socket.getsockname()) for server_socket in (self._server.sockets or ())
        )
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
                snapshot_max_bytes=self.config.persistence_max_load_bytes,
                masterauth=self.config.masterauth,
            )
            self._bg_tasks.append(self._replica.start())

        self._bg_tasks.append(asyncio.create_task(self._active_expire_loop(), name="active-expire"))
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
        if self._aof_rewrite_task is not None and not self._aof_rewrite_task.done():
            try:
                await self._aof_rewrite_task
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

        # 关闭现有连接，确保停止命令不会留下悬挂客户端或副本连接。
        writers = list(self._writers.values())
        for writer in writers:
            writer.close()
        if writers:
            await asyncio.gather(
                *(writer.wait_closed() for writer in writers),
                return_exceptions=True,
            )
        client_tasks = [task for task in self._client_tasks if not task.done()]
        if client_tasks:
            await asyncio.gather(*client_tasks, return_exceptions=True)

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
                cfg.aof_path,
                apply,
                max_bulk_len=cfg.proto_max_bulk_len,
                max_file_bytes=cfg.persistence_max_load_bytes,
            )
            return
        if cfg.rdb_path.exists() and cfg.rdb_path.stat().st_size > 0:
            load_rdb_into(
                self.store,
                cfg.rdb_path,
                max_file_bytes=cfg.persistence_max_load_bytes,
            )
            return
        logger.info("No persistence files found; starting with empty DB")

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        if self._client_count >= self.config.max_clients:
            self._total_connections_received += 1
            self._rejected_connections += 1
            writer.write(encode_error("ERR max number of clients reached"))
            try:
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
            return
        self._total_connections_received += 1
        self._client_count += 1
        current_task: asyncio.Task[None] | None = asyncio.current_task()
        if current_task is not None:
            self._client_tasks.add(current_task)
        self._conn_seq += 1
        conn_id = self._conn_seq
        self._writers[conn_id] = writer
        peer = writer.get_extra_info("peername")
        logger.debug("Client %d connected from %s", conn_id, peer)
        parser = RespParser(
            max_bulk_len=self.config.proto_max_bulk_len,
            max_buffer_bytes=self.max_buffer_bytes,
            max_array_len=self.config.proto_max_array_len,
            max_array_depth=self.config.max_array_depth,
        )
        became_replica = False
        partial_since: float | None = None
        try:
            while True:
                timeout = self.config.client_idle_timeout_sec
                if parser.buffered_size > 0 and partial_since is not None:
                    remaining = self.partial_timeout_sec - (time.monotonic() - partial_since)
                    if remaining <= 0:
                        raise ProtocolError(
                            f"incomplete RESP frame timed out after {self.partial_timeout_sec:.0f}s"
                        )
                    timeout = min(timeout, remaining)
                try:
                    data = await asyncio.wait_for(reader.read(65536), timeout=timeout)
                except TimeoutError as exc:
                    if parser.buffered_size == 0:
                        logger.info("Client %d idle timeout", conn_id)
                        break
                    raise ProtocolError(
                        f"incomplete RESP frame timed out after {self.partial_timeout_sec:.0f}s"
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
                    self._total_commands_processed += 1
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
                    if (
                        cmd == b"AUTH"
                        and self._authentication_failures.get(conn_id, 0)
                        >= self.config.max_auth_failures
                    ):
                        logger.warning("Closing conn %d after AUTH failures", conn_id)
                        return
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
            self._authenticated_connections.discard(conn_id)
            self._authentication_failures.pop(conn_id, None)
            if current_task is not None:
                self._client_tasks.discard(current_task)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            self._client_count -= 1
            logger.debug("Client %d closed", conn_id)

    async def _hold_replica_connection(self, reader: asyncio.StreamReader, conn_id: int) -> None:
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                logger.debug("Ignoring %d bytes from replica conn %d", len(data), conn_id)
        except (ConnectionError, OSError):
            pass

    def _normalize_argv(self, msg: object) -> list[bytes]:
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
                raise ProtocolError(f"command arg must be bulk string, got {type(item)}")
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
        upper = argv[0].upper() if argv else b""
        if upper in WRITE_COMMANDS:
            async with self._write_lock:
                return await self._execute_inner(
                    argv,
                    connection_id=connection_id,
                    is_replica_client=is_replica_client,
                    is_loading=is_loading,
                    is_replica_link=is_replica_link,
                )
        return await self._execute_inner(
            argv,
            connection_id=connection_id,
            is_replica_client=is_replica_client,
            is_loading=is_loading,
            is_replica_link=is_replica_link,
        )

    async def _execute_inner(
        self,
        argv: list[bytes],
        *,
        connection_id: int,
        is_replica_client: bool,
        is_loading: bool,
        is_replica_link: bool,
    ) -> bytes | None:
        """在需要时由写屏障保护，完成命令执行和持久化后处理。"""
        upper = argv[0].upper() if argv else b""
        if (
            self.config.requirepass is not None
            and connection_id not in self._authenticated_connections
            and upper != b"AUTH"
            and not is_loading
            and not is_replica_client
        ):
            self._total_commands_failed += 1
            return encode_error("NOAUTH Authentication required.")
        if (
            upper in WRITE_COMMANDS
            and self.config.aof_enabled
            and not self.aof.healthy
            and not is_loading
            and not is_replica_client
        ):
            self._total_commands_failed += 1
            return encode_error("MISCONF AOF persistence is unhealthy")
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
        before_memory = self.store.snapshot() if upper in WRITE_COMMANDS else None
        try:
            result = await self.registry.dispatch(ctx)
        except RespError as exc:
            self._total_commands_failed += 1
            return exc.to_resp()
        except Exception:
            self._total_commands_failed += 1
            logger.exception("Internal error handling %r", argv[0] if argv else None)
            return encode_error("ERR internal error")

        evicted_keys: list[bytes] = []
        if upper in WRITE_COMMANDS:
            try:
                evicted_keys = self.store.enforce_memory_limit()
            except MemoryLimitError as exc:
                if before_memory is not None:
                    self.store.load_entries(before_memory)
                self._total_commands_failed += 1
                return encode_error(str(exc))

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
                await self.aof.append(argv)
            except OSError as exc:
                self._total_commands_failed += 1
                logger.exception("AOF append failed")
                return encode_error(f"MISCONF AOF persistence failed: {exc}")
            # Non-blocking: must not await per-replica network completion
            self.master.propagate(argv)
            for key in evicted_keys:
                eviction_command = [b"DEL", key]
                try:
                    await self.aof.append(eviction_command)
                except OSError as exc:
                    self._total_commands_failed += 1
                    logger.exception("AOF append failed while recording eviction")
                    return encode_error(f"MISCONF AOF persistence failed: {exc}")
                self.master.propagate(eviction_command)
        elif upper in WRITE_COMMANDS and is_replica_client and not is_loading:
            try:
                await self.aof.append(argv)
            except OSError as exc:
                self._total_commands_failed += 1
                logger.exception("AOF append failed on replica")
                return encode_error(f"MISCONF AOF persistence failed: {exc}")

        try:
            return encode_value(result)
        except TypeError:
            logger.exception("Failed to encode result for %r", argv[0])
            return encode_error("ERR internal error")

    def authenticate(self, connection_id: int, password: bytes) -> bool:
        """常量时间比较密码，成功后标记当前连接已认证。"""
        configured = self.config.requirepass
        if configured is None:
            return False
        expected = configured.encode("utf-8")
        if not hmac.compare_digest(expected, password):
            self._authentication_failures[connection_id] = (
                self._authentication_failures.get(connection_id, 0) + 1
            )
            return False
        self._authenticated_connections.add(connection_id)
        self._authentication_failures.pop(connection_id, None)
        return True

    def _validate_network_security(self) -> None:
        """无认证时禁止意外监听非回环地址。"""
        for name, filename in (
            ("dbfilename", self.config.dbfilename),
            ("aof-filename", self.config.aof_filename),
        ):
            path = os.path.normpath(filename)
            if not filename or os.path.isabs(filename) or path != os.path.basename(path):
                raise ValueError(f"{name} must be a plain filename")
        host = self.config.host.strip().lower()
        is_loopback = host == "localhost"
        try:
            is_loopback = is_loopback or ipaddress.ip_address(host).is_loopback
        except ValueError:
            pass
        if (
            not is_loopback
            and self.config.requirepass is None
            and not self.config.allow_unprotected_non_loopback
        ):
            raise ValueError(
                "Refusing non-loopback bind without AUTH; configure --requirepass "
                "or explicitly use --allow-unprotected-non-loopback"
            )

    async def _apply_replicated_command(self, argv: list[bytes]) -> None:
        await self._execute(argv, is_replica_client=True)

    def save_rdb_sync(self) -> None:
        dump_rdb(self.store, self.config.rdb_path)
        self.last_save_time = int(time.time())

    def schedule_bgsave(self) -> bool:
        """Start a background BGSAVE task. Returns False if already in progress."""
        if self.bgsave_in_progress:
            return False
        self._bgsave_task = asyncio.create_task(self._bgsave_worker(), name="bgsave")
        return True

    async def _bgsave_worker(self) -> None:
        async with self._bgsave_lock:
            try:
                # 快照必须和写命令使用同一写屏障，随后再移出事件循环编码。
                async with self._write_lock:
                    entries = self.store.snapshot()
                payload = await asyncio.to_thread(dump_rdb_entries, entries)
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

    def schedule_aof_rewrite(self) -> bool:
        """启动 AOF 重写；已有任务运行时返回假。"""
        if self.aof_rewrite_in_progress or not self.config.aof_enabled:
            return False
        self._aof_rewrite_task = asyncio.create_task(self._aof_rewrite_worker(), name="aof-rewrite")
        return True

    async def _aof_rewrite_worker(self) -> None:
        """在写屏障内生成并原子替换压缩后的 AOF。"""
        async with self._aof_rewrite_lock:
            try:
                async with self._write_lock:
                    entries = self.store.snapshot()
                    size = await self.aof.rewrite(entries)
                logger.info("BGREWRITEAOF completed (%d bytes)", size)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("BGREWRITEAOF failed")

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
