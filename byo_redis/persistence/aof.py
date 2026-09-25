"""AOF append-only log with RESP command records.

Write path is non-blocking relative to peer command handling: ``append``
enqueues encoded payloads; a background task performs write+fsync on a worker
thread, preserving command order and always/everysec/no semantics.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from byo_redis.protocol.encoder import encode_command
from byo_redis.protocol.parser import ProtocolError, RespParser
from byo_redis.storage.expiry import now_ms
from byo_redis.storage.types import KeyEntry, RedisType

logger = logging.getLogger(__name__)


class AofError(Exception):
    """AOF 文件损坏或无法安全重放。"""


@dataclass
class _AofItem:
    payload: bytes
    completed: asyncio.Future[None]


class AOFLog:
    """带有界队列和持久化确认的 AOF 管理器。"""

    def __init__(
        self,
        path: Path,
        *,
        fsync_policy: str = "everysec",
        enabled: bool = True,
        queue_max_commands: int = 10_000,
        enqueue_timeout_sec: float = 5.0,
    ) -> None:
        self.path = path
        self.fsync_policy = fsync_policy
        self.enabled = enabled
        self.queue_max_commands = queue_max_commands
        self.enqueue_timeout_sec = enqueue_timeout_sec
        self._fh: BinaryIO | None = None
        self._dirty = False
        self._fsync_task: asyncio.Task[None] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._queue: asyncio.Queue[_AofItem | None] | None = None
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._failure: OSError | None = None
        self._io_lock = threading.Lock()
        self.last_fsync_time: float = 0.0

    @property
    def pending_commands(self) -> int:
        """返回尚未处理的 AOF 命令数量。"""
        return 0 if self._queue is None else self._queue.qsize()

    @property
    def healthy(self) -> bool:
        """AOF writer 尚未出现不可恢复错误时返回真。"""
        return self._failure is None

    def open(self) -> None:
        if not self.enabled:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "ab", buffering=0)
        self._closed = False
        self._queue = asyncio.Queue(maxsize=self.queue_max_commands)
        self._failure = None
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None

    async def start_background_fsync(self) -> None:
        if not self.enabled:
            return
        if self._queue is None:
            self._queue = asyncio.Queue(maxsize=self.queue_max_commands)
        self._loop = asyncio.get_running_loop()
        self._writer_task = asyncio.create_task(self._writer_loop(), name="aof-writer")
        if self.fsync_policy == "everysec":
            self._fsync_task = asyncio.create_task(self._fsync_loop(), name="aof-fsync")

    async def _writer_loop(self) -> None:
        assert self._queue is not None
        try:
            while True:
                item = await self._queue.get()
                if item is None:
                    break
                try:
                    await asyncio.to_thread(self._write_sync, item.payload)
                    if self.fsync_policy == "always":
                        # always 模式必须在命令响应前完成 fsync。
                        await asyncio.to_thread(self._fsync_sync)
                except OSError as exc:
                    self._failure = exc
                    if not item.completed.done():
                        item.completed.set_exception(exc)
                    await self._fail_pending(exc)
                    logger.exception("AOF writer stopped after persistence failure")
                    return
                if not item.completed.done():
                    item.completed.set_result(None)
        except asyncio.CancelledError:
            cancel_error = OSError("AOF writer task cancelled")
            await self._fail_pending(cancel_error)
            raise

    async def _fail_pending(self, exc: OSError) -> None:
        """将队列中尚未写入的命令全部标记为失败。"""
        if self._queue is None:
            return
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            if item is not None and not item.completed.done():
                item.completed.set_exception(exc)

    def _write_sync(self, payload: bytes) -> None:
        if self._fh is None:
            raise OSError("AOF is not open")
        with self._io_lock:
            self._fh.write(payload)
            self._dirty = True

    def _fsync_sync(self) -> None:
        if self._fh is None or not self._dirty:
            return
        with self._io_lock:
            os.fsync(self._fh.fileno())
            self._dirty = False
            self.last_fsync_time = time.time()

    async def _fsync_loop(self) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(1.0)
                try:
                    await asyncio.to_thread(self._fsync_sync)
                except OSError as exc:
                    self._failure = exc
                    await self._fail_pending(exc)
                    logger.exception("AOF periodic fsync failed")
                    return
        except asyncio.CancelledError:
            return

    async def append(self, argv: list[bytes]) -> None:
        """写入 AOF；always 模式在返回前确认 fsync 已完成。"""
        if not self.enabled or self._fh is None:
            return
        if self._failure is not None:
            raise OSError(f"AOF is unhealthy: {self._failure}") from self._failure
        payload = encode_command(argv)
        if self._queue is None or self._writer_task is None:
            await asyncio.to_thread(self._write_sync, payload)
            if self.fsync_policy == "always":
                await asyncio.to_thread(self._fsync_sync)
            return

        completed = asyncio.get_running_loop().create_future()
        item = _AofItem(payload=payload, completed=completed)
        try:
            await asyncio.wait_for(self._queue.put(item), timeout=self.enqueue_timeout_sec)
        except TimeoutError as exc:
            raise OSError("AOF queue is full") from exc
        await completed

    async def rewrite(self, entries: dict[bytes, KeyEntry]) -> int:
        """将当前键空间压缩成新的 AOF，并原子替换旧文件。"""
        if not self.enabled:
            raise OSError("AOF is disabled")
        payload = await asyncio.to_thread(_build_rewrite_payload, entries)
        await asyncio.to_thread(self._replace_file_sync, payload)
        return len(payload)

    def _replace_file_sync(self, payload: bytes) -> None:
        """写入临时文件并在 I/O 锁内切换当前 AOF 文件句柄。"""
        tmp = self.path.with_suffix(self.path.suffix + ".rewrite.tmp")
        with open(tmp, "wb") as rewrite_file:
            rewrite_file.write(payload)
            rewrite_file.flush()
            os.fsync(rewrite_file.fileno())
        with self._io_lock:
            old_file = self._fh
            try:
                if old_file is not None:
                    os.fsync(old_file.fileno())
                    old_file.close()
                os.replace(tmp, self.path)
                self._fh = open(self.path, "ab", buffering=0)
                self._dirty = False
                self.last_fsync_time = time.time()
            except OSError:
                if self._fh is None or self._fh.closed:
                    self._fh = open(self.path, "ab", buffering=0)
                raise

    async def close(self) -> None:
        self._closed = True
        if (
            self._queue is not None
            and self._writer_task is not None
            and not self._writer_task.done()
        ):
            await self._queue.put(None)
        if self._writer_task is not None:
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass
            self._writer_task = None
        if self._fsync_task is not None:
            self._fsync_task.cancel()
            try:
                await self._fsync_task
            except asyncio.CancelledError:
                pass
            self._fsync_task = None
        if self._fh is not None:
            try:
                try:
                    self._fsync_sync()
                except OSError:
                    logger.exception("AOF final fsync failed during shutdown")
                self._fh.close()
            finally:
                self._fh = None
        self._queue = None

    @staticmethod
    def read_commands(
        path: Path,
        *,
        max_bulk_len: int = 16_777_216,
        max_file_bytes: int = 1024 * 1024 * 1024,
    ) -> list[list[bytes]]:
        """Parse AOF into command argv lists; truncate trailing corrupt bytes."""
        if not path.exists() or path.stat().st_size == 0:
            return []
        file_size = path.stat().st_size
        if file_size > max_file_bytes:
            raise AofError(f"AOF size {file_size} exceeds load limit {max_file_bytes}")
        data = path.read_bytes()
        parser = RespParser(max_bulk_len=max_bulk_len)
        try:
            messages = parser.feed(data)
        except ProtocolError as exc:
            raise AofError(f"AOF protocol error: {exc}") from exc

        residual = parser.buffered_size
        if residual:
            logger.warning(
                "AOF has %d trailing incomplete bytes; ignoring corrupt tail",
                residual,
            )

        commands: list[list[bytes]] = []
        for msg in messages:
            if not isinstance(msg, list) or not msg:
                logger.warning("Skipping non-array AOF entry: %r", msg)
                continue
            if not all(isinstance(x, (bytes, bytearray)) or x is None for x in msg):
                argv: list[bytes] = []
                ok = True
                for x in msg:
                    if isinstance(x, (bytes, bytearray)):
                        argv.append(bytes(x))
                    elif isinstance(x, str):
                        argv.append(x.encode("utf-8"))
                    else:
                        ok = False
                        break
                if not ok:
                    logger.warning("Skipping malformed AOF command: %r", msg)
                    continue
                commands.append(argv)
            else:
                commands.append([b"" if x is None else bytes(x) for x in msg])
        return commands


async def replay_aof(
    path: Path,
    apply: Callable[[list[bytes]], Awaitable[None]],
    *,
    max_bulk_len: int = 16_777_216,
    max_file_bytes: int = 1024 * 1024 * 1024,
) -> int:
    commands = AOFLog.read_commands(
        path,
        max_bulk_len=max_bulk_len,
        max_file_bytes=max_file_bytes,
    )
    for argv in commands:
        await apply(argv)
    logger.info("AOF replayed %d commands from %s", len(commands), path)
    return len(commands)


def _build_rewrite_payload(entries: dict[bytes, KeyEntry]) -> bytes:
    """把快照转换成能恢复等价状态的最小命令序列。"""
    commands: list[bytes] = []
    current_ms = now_ms()
    for key, entry in entries.items():
        if entry.type is RedisType.STRING:
            assert isinstance(entry.value, bytes)
            commands.append(encode_command([b"SET", key, entry.value]))
        elif entry.type is RedisType.LIST:
            assert not isinstance(entry.value, (bytes, dict))
            values = list(reversed(entry.value))
            if values:
                commands.append(encode_command([b"LPUSH", key, *values]))
        elif entry.type is RedisType.HASH:
            assert isinstance(entry.value, dict)
            pairs: list[bytes] = []
            for field, value in entry.value.items():
                pairs.extend((field, value))
            if pairs:
                commands.append(encode_command([b"HSET", key, *pairs]))
        if entry.expire_at_ms is not None:
            remaining_ms = entry.expire_at_ms - current_ms
            if remaining_ms > 0:
                seconds = max(1, math.ceil(remaining_ms / 1000))
                commands.append(encode_command([b"EXPIRE", key, str(seconds).encode("ascii")]))
    return b"".join(commands)
