"""Replica client: connect to master, FULLRESYNC, apply command stream."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from byo_redis.persistence.rdb import load_rdb_bytes
from byo_redis.protocol.encoder import encode_command
from byo_redis.protocol.parser import ProtocolError, RespParser
from byo_redis.storage.store import Store

logger = logging.getLogger(__name__)

ApplyCommand = Callable[[list[bytes]], Awaitable[None]]


class ReplicaClient:
    def __init__(
        self,
        host: str,
        port: int,
        store: Store,
        apply_command: ApplyCommand,
        *,
        listening_port: int = 0,
        max_bulk_len: int = 16_777_216,
        snapshot_max_bytes: int = 1024 * 1024 * 1024,
        masterauth: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.store = store
        self.apply_command = apply_command
        self.listening_port = listening_port
        self.max_bulk_len = max_bulk_len
        self.snapshot_max_bytes = snapshot_max_bytes
        self.masterauth = masterauth
        self.link_status = "down"
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._parser: RespParser | None = None

    def start(self) -> asyncio.Task[None]:
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="replica-client")
        return self._task

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self.link_status = "down"

    async def _run(self) -> None:
        backoff = 0.5
        while not self._stop.is_set():
            try:
                await self._session()
                backoff = 0.5
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Replica session to %s:%d failed; retrying in %.1fs",
                    self.host,
                    self.port,
                    backoff,
                )
                self.link_status = "down"
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                    return
                except TimeoutError:
                    backoff = min(backoff * 2, 10.0)

    async def _session(self) -> None:
        logger.info("Connecting to master %s:%d", self.host, self.port)
        reader, writer = await asyncio.open_connection(self.host, self.port)
        parser = RespParser(max_bulk_len=self.max_bulk_len)
        try:
            await self._handshake(reader, writer, parser)
            await self._read_loop(reader, parser)
        finally:
            self.link_status = "down"
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _handshake(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        parser: RespParser,
    ) -> None:
        async def send_cmd(*parts: bytes) -> None:
            writer.write(encode_command(list(parts)))
            await writer.drain()

        async def read_one() -> object:
            while True:
                data = await reader.read(65536)
                if not data:
                    raise ConnectionError("master closed during handshake")
                msgs = parser.feed(data)
                if msgs:
                    if len(msgs) > 1:
                        raise ProtocolError("unexpected extra handshake messages")
                    return msgs[0]

        if self.masterauth is not None:
            await send_cmd(b"AUTH", self.masterauth.encode("utf-8"))
            auth_reply = await read_one()
            if auth_reply != "OK" and auth_reply != b"OK":
                raise ProtocolError(f"unexpected AUTH reply: {auth_reply!r}")

        await send_cmd(b"PING")
        pong = await read_one()
        if pong != "PONG" and pong != b"PONG":
            raise ProtocolError(f"unexpected PING reply: {pong!r}")

        await send_cmd(
            b"REPLCONF",
            b"listening-port",
            str(self.listening_port).encode("ascii"),
        )
        ok1 = await read_one()
        if ok1 != "OK" and ok1 != b"OK":
            raise ProtocolError(f"unexpected REPLCONF reply: {ok1!r}")

        await send_cmd(b"REPLCONF", b"capa", b"eof")
        ok2 = await read_one()
        if ok2 != "OK" and ok2 != b"OK":
            raise ProtocolError(f"unexpected REPLCONF capa reply: {ok2!r}")

        await send_cmd(b"PSYNC", b"?", b"-1")
        full = await read_one()
        if not isinstance(full, str) or not full.startswith("FULLRESYNC "):
            raise ProtocolError(f"expected FULLRESYNC, got {full!r}")
        logger.info("Master replied %s", full)

        rdb = await self._read_rdb_bulk(reader, parser)
        entries = load_rdb_bytes(
            rdb, max_file_bytes=self.snapshot_max_bytes
        )
        self.store.load_entries(entries)
        self.link_status = "up"
        logger.info("FULLRESYNC complete: loaded %d keys from master", len(entries))

    async def _read_rdb_bulk(
        self, reader: asyncio.StreamReader, parser: RespParser
    ) -> bytes:
        """Read Redis-style RDB bulk: $<len>\\r\\n + raw bytes (no trailing CRLF)."""
        buf = bytearray()
        if parser.buffered_size:
            buf.extend(parser._buf)
            parser.reset()

        async def ensure(n: int) -> None:
            while len(buf) < n:
                chunk = await reader.read(65536)
                if not chunk:
                    raise ConnectionError("master closed while sending RDB")
                buf.extend(chunk)

        await ensure(1)
        if buf[0] != ord("$"):
            raise ProtocolError(f"expected RDB bulk header, got {buf[:20]!r}")
        while True:
            idx = buf.find(b"\r\n")
            if idx >= 0:
                break
            await ensure(len(buf) + 1)
        header = bytes(buf[1:idx])
        try:
            length = int(header)
        except ValueError as exc:
            raise ProtocolError(f"bad RDB bulk length {header!r}") from exc
        if length < 0:
            raise ProtocolError("null RDB not allowed")
        if length > self.snapshot_max_bytes:
            raise ProtocolError(
                f"RDB bulk length {length} exceeds snapshot limit "
                f"{self.snapshot_max_bytes}"
            )
        start = idx + 2
        await ensure(start + length)
        rdb = bytes(buf[start : start + length])
        leftover = bytes(buf[start + length :])
        if leftover:
            parser.feed(leftover)
        return rdb

    async def _read_loop(
        self, reader: asyncio.StreamReader, parser: RespParser
    ) -> None:
        # Apply any complete commands already buffered after RDB
        await self._apply_messages(parser.feed(b""))

        while not self._stop.is_set():
            data = await reader.read(65536)
            if not data:
                raise ConnectionError("master closed connection")
            try:
                messages = parser.feed(data)
            except ProtocolError:
                logger.exception("Protocol error on replication stream")
                raise
            await self._apply_messages(messages)

    async def _apply_messages(self, messages: list[object]) -> None:
        for msg in messages:
            if not isinstance(msg, list) or not msg:
                logger.warning("Ignoring non-command on repl stream: %r", msg)
                continue
            argv: list[bytes] = []
            for item in msg:
                if isinstance(item, (bytes, bytearray)):
                    argv.append(bytes(item))
                elif isinstance(item, str):
                    argv.append(item.encode("utf-8"))
                elif item is None:
                    argv.append(b"")
                else:
                    raise ProtocolError(f"bad argv element: {item!r}")
            await self.apply_command(argv)
