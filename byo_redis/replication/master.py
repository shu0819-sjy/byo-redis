"""Master-side replication: track replicas and propagate writes."""

from __future__ import annotations

import asyncio
import logging
import secrets
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from byo_redis.persistence.rdb import dump_rdb_bytes
from byo_redis.protocol.encoder import encode_command, encode_simple_string

if TYPE_CHECKING:
    from byo_redis.storage.store import Store

logger = logging.getLogger(__name__)

# Sentinel values for the per-replica drain queue
_DRAIN = object()
_STOP = object()

ReplicaState = Literal["syncing", "live"]


@dataclass
class ReplicaLink:
    writer: asyncio.StreamWriter
    connection_id: int
    offset: int = 0
    alive: bool = True
    state: ReplicaState = "live"
    # Ordered backlog of already-encoded RESP command payloads while syncing
    backlog: list[bytes] = field(default_factory=list)
    _drain_task: asyncio.Task[None] | None = field(default=None, repr=False)
    _pending: asyncio.Queue[object] = field(default_factory=asyncio.Queue, repr=False)


@dataclass
class MasterReplication:
    store: "Store"
    replid: str = field(default_factory=lambda: secrets.token_hex(20))
    offset: int = 0
    replicas: dict[int, ReplicaLink] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    # Protects link.state / link.backlog / offset updates shared by sync
    # ``propagate`` and async ``full_resync`` (must not be held across await).
    _state_lock: threading.Lock = field(default_factory=threading.Lock)
    # Optional awaitable hook after syncing register, before RDB write.
    full_resync_pause_hook: Callable[[], Awaitable[None]] | None = None
    # Optional awaitable hook inside the flush-until-empty loop (after a batch
    # is written, before drain) — tests inject concurrent SETs in the flush window.
    full_resync_flush_hook: Callable[[], Awaitable[None]] | None = None

    @property
    def connected_replicas(self) -> int:
        return sum(1 for r in self.replicas.values() if r.alive and r.state == "live")

    async def register_syncing(
        self, connection_id: int, writer: asyncio.StreamWriter
    ) -> ReplicaLink:
        """Register a replica in ``syncing`` state (buffers propagate → backlog)."""
        link = ReplicaLink(
            writer=writer,
            connection_id=connection_id,
            offset=self.offset,
            state="syncing",
        )
        async with self._lock:
            with self._state_lock:
                self.replicas[connection_id] = link
        logger.info("Replica connection %d registered (syncing)", connection_id)
        return link

    def _start_drain_task(self, link: ReplicaLink) -> None:
        if link._drain_task is not None:
            return
        link._drain_task = asyncio.create_task(
            self._replica_writer_loop(link),
            name=f"replica-writer-{link.connection_id}",
        )

    async def register(
        self, connection_id: int, writer: asyncio.StreamWriter
    ) -> ReplicaLink:
        """Register a live replica (used when not going through full_resync)."""
        link = ReplicaLink(
            writer=writer,
            connection_id=connection_id,
            offset=self.offset,
            state="live",
        )
        self._start_drain_task(link)
        async with self._lock:
            with self._state_lock:
                self.replicas[connection_id] = link
        logger.info("Replica connection %d registered (live)", connection_id)
        return link

    async def unregister(self, connection_id: int) -> None:
        async with self._lock:
            with self._state_lock:
                link = self.replicas.pop(connection_id, None)
        if link is None:
            return
        link.alive = False
        try:
            link._pending.put_nowait(_STOP)
        except Exception:
            pass
        if link._drain_task is not None:
            link._drain_task.cancel()
            try:
                await link._drain_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            link._drain_task = None
        logger.info("Replica connection %d unregistered", connection_id)

    async def full_resync(
        self, writer: asyncio.StreamWriter, connection_id: int
    ) -> None:
        """FULLRESYNC with syncing backlog + flush-until-empty live transition.

        1. Snapshot store → RDB bytes
        2. Register link in ``syncing`` (propagate appends to ordered backlog)
        3. Optional pause hook (mid-sync concurrent SETs)
        4. Send FULLRESYNC header + RDB, drain
        5. **Flush-until-empty**: under ``_state_lock``, take backlog batch or
           atomically mark ``live`` if empty; write batch; optional flush hook;
           drain; repeat until live.
        6. Start drain task

        Holding the decision to continue vs go live inside ``_state_lock`` (shared
        with ``propagate``) closes R3-BACKLOG-FLUSH-GAP: writes that arrive during
        ``await drain`` stay on the syncing backlog and are flushed on the next
        loop iteration instead of being stranded after a premature live flip.
        """
        rdb = dump_rdb_bytes(self.store)
        header = encode_simple_string(f"FULLRESYNC {self.replid} {self.offset}")
        bulk_hdr = f"${len(rdb)}\r\n".encode("ascii")
        payload = header + bulk_hdr + rdb

        registered = False
        total_flushed = 0
        try:
            link = await self.register_syncing(connection_id, writer)
            registered = True

            if self.full_resync_pause_hook is not None:
                await self.full_resync_pause_hook()

            writer.write(payload)
            await writer.drain()

            # Flush-until-empty + atomic live transition
            while True:
                with self._state_lock:
                    batch = list(link.backlog)
                    link.backlog.clear()
                    if not batch:
                        link.state = "live"
                        link.offset = self.offset
                        go_live = True
                    else:
                        go_live = False

                if go_live:
                    break

                for cmd_payload in batch:
                    writer.write(cmd_payload)
                total_flushed += len(batch)

                # Flush-window hook: concurrent propagate still sees syncing
                # and appends to backlog; next loop iteration will flush them.
                if self.full_resync_flush_hook is not None:
                    await self.full_resync_flush_hook()

                await writer.drain()

            self._start_drain_task(link)

            logger.info(
                "FULLRESYNC sent to conn %d (%d RDB bytes, backlog_flushed=%d, "
                "replid=%s offset=%d)",
                connection_id,
                len(rdb),
                total_flushed,
                self.replid,
                self.offset,
            )
        except Exception:
            if registered:
                await self.unregister(connection_id)
            raise

    def propagate(self, argv: list[bytes]) -> None:
        """Non-blocking propagate for the client write path.

        Syncing replicas buffer into an ordered backlog; live replicas get
        ``writer.write()`` immediately with drain scheduled on a background task.
        State/backlog mutations share ``_state_lock`` with ``full_resync``.
        """
        if not self.replicas:
            return
        payload = encode_command(argv)
        dead: list[int] = []
        with self._state_lock:
            self.offset += len(payload)
            items = list(self.replicas.items())
            for conn_id, link in items:
                if not link.alive:
                    dead.append(conn_id)
                    continue
                if link.state == "syncing":
                    link.backlog.append(payload)
                    link.offset = self.offset
                    continue
                try:
                    link.writer.write(payload)
                    link.offset = self.offset
                    link._pending.put_nowait(_DRAIN)
                except (ConnectionError, OSError, RuntimeError) as exc:
                    logger.warning(
                        "Failed to propagate to replica %d: %s", conn_id, exc
                    )
                    link.alive = False
                    dead.append(conn_id)
        for conn_id in dead:
            asyncio.create_task(
                self.unregister(conn_id), name=f"replica-unreg-{conn_id}"
            )

    async def _replica_writer_loop(self, link: ReplicaLink) -> None:
        """Background drain for a live replica transport."""
        try:
            while link.alive:
                item = await link._pending.get()
                if item is _STOP:
                    break
                if item is _DRAIN:
                    try:
                        await link.writer.drain()
                    except (ConnectionError, OSError) as exc:
                        logger.warning(
                            "Replica %d drain failed: %s",
                            link.connection_id,
                            exc,
                        )
                        link.alive = False
                        asyncio.create_task(
                            self.unregister(link.connection_id),
                            name=f"replica-unreg-{link.connection_id}",
                        )
                        break
        except asyncio.CancelledError:
            return
