"""AOF append-only log with RESP command records.

Write path is non-blocking relative to peer command handling: ``append``
enqueues encoded payloads; a background task performs write+fsync on a worker
thread, preserving command order and always/everysec/no semantics.
"""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Awaitable, Callable

from byo_redis.protocol.encoder import encode_command
from byo_redis.protocol.parser import ProtocolError, RespParser

logger = logging.getLogger(__name__)


class AOFLog:
    """Append-only file manager with background flush queue."""

    def __init__(
        self,
        path: Path,
        *,
        fsync_policy: str = "everysec",
        enabled: bool = True,
    ) -> None:
        self.path = path
        self.fsync_policy = fsync_policy
        self.enabled = enabled
        self._fh: object | None = None
        self._dirty = False
        self._fsync_task: asyncio.Task[None] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._queue: asyncio.Queue[bytes | None] | None = None
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None

    def open(self) -> None:
        if not self.enabled:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "ab", buffering=0)
        self._closed = False
        self._queue = asyncio.Queue()
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None

    async def start_background_fsync(self) -> None:
        if not self.enabled:
            return
        if self._queue is None:
            self._queue = asyncio.Queue()
        self._loop = asyncio.get_running_loop()
        self._writer_task = asyncio.create_task(
            self._writer_loop(), name="aof-writer"
        )
        if self.fsync_policy == "everysec":
            self._fsync_task = asyncio.create_task(
                self._fsync_loop(), name="aof-fsync"
            )

    async def _writer_loop(self) -> None:
        assert self._queue is not None
        try:
            while True:
                item = await self._queue.get()
                if item is None:
                    break
                await asyncio.to_thread(self._write_sync, item)
                if self.fsync_policy == "always":
                    await asyncio.to_thread(self._fsync_sync)
        except asyncio.CancelledError:
            # Drain remaining queued payloads best-effort
            if self._queue is not None:
                while not self._queue.empty():
                    item = self._queue.get_nowait()
                    if item is None:
                        break
                    try:
                        self._write_sync(item)
                    except OSError:
                        logger.exception("AOF drain write failed")
            return

    def _write_sync(self, payload: bytes) -> None:
        if self._fh is None:
            return
        self._fh.write(payload)  # type: ignore[attr-defined]
        self._dirty = True

    def _fsync_sync(self) -> None:
        if self._fh is None or not self._dirty:
            return
        try:
            os.fsync(self._fh.fileno())  # type: ignore[attr-defined]
            self._dirty = False
        except OSError:
            logger.exception("AOF fsync failed")

    async def _fsync_loop(self) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(1.0)
                await asyncio.to_thread(self._fsync_sync)
        except asyncio.CancelledError:
            return

    def append(self, argv: list[bytes]) -> None:
        """Enqueue a write command. Returns immediately (non-blocking for peers)."""
        if not self.enabled or self._fh is None:
            return
        payload = encode_command(argv)
        if self._queue is not None:
            try:
                self._queue.put_nowait(payload)
                return
            except Exception:
                logger.exception("AOF enqueue failed; falling back to sync write")
        # Fallback if queue not started yet (e.g. during early bootstrap)
        try:
            self._write_sync(payload)
            if self.fsync_policy == "always":
                self._fsync_sync()
        except OSError:
            logger.exception("AOF append failed")

    async def close(self) -> None:
        self._closed = True
        if self._queue is not None:
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
                self._fsync_sync()
                self._fh.close()  # type: ignore[attr-defined]
            finally:
                self._fh = None
        self._queue = None

    @staticmethod
    def read_commands(
        path: Path, *, max_bulk_len: int = 16_777_216
    ) -> list[list[bytes]]:
        """Parse AOF into command argv lists; truncate trailing corrupt bytes."""
        if not path.exists() or path.stat().st_size == 0:
            return []
        data = path.read_bytes()
        parser = RespParser(max_bulk_len=max_bulk_len)
        try:
            messages = parser.feed(data)
        except ProtocolError as exc:
            logger.warning("AOF protocol error, attempting salvage: %s", exc)
            messages = _salvage_parse(data, max_bulk_len=max_bulk_len)

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
            if not all(
                isinstance(x, (bytes, bytearray)) or x is None for x in msg
            ):
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
                commands.append([bytes(x) for x in msg])
        return commands


def _salvage_parse(data: bytes, *, max_bulk_len: int) -> list:
    """Parse as many complete RESP values as possible from the prefix."""
    parser = RespParser(max_bulk_len=max_bulk_len)
    best: list = []
    i = 0
    while i < len(data):
        nxt = data.find(b"\n", i)
        if nxt < 0:
            break
        chunk = data[i : nxt + 1]
        i = nxt + 1
        try:
            msgs = parser.feed(chunk)
            best.extend(msgs)
        except ProtocolError:
            logger.warning("AOF salvage stopped at offset %d", i - len(chunk))
            break
    return best


async def replay_aof(
    path: Path,
    apply: Callable[[list[bytes]], Awaitable[None]],
    *,
    max_bulk_len: int = 16_777_216,
) -> int:
    commands = AOFLog.read_commands(path, max_bulk_len=max_bulk_len)
    for argv in commands:
        await apply(argv)
    logger.info("AOF replayed %d commands from %s", len(commands), path)
    return len(commands)
