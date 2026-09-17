"""Integration: master/replica full sync and consistency.

Also hosts repair-round-3 coverage that must live in this in-scope file:
- writes during FULLRESYNC
- concurrent BGSAVE rejection
- parser max_buffer_bytes ProtocolError
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from byo_redis.config import Config
from byo_redis.protocol.parser import ProtocolError, ProtocolErrorMsg, RespParser
from byo_redis.replication.replica import ReplicaClient
from byo_redis.server import RedisServer
from byo_redis.storage.store import Store
from tests.conftest import RespClient, unused_port


async def _wait_replica_up(replica: RedisServer, timeout: float = 5.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if replica.replica_link_status == "up":
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"replica link not up: {replica.replica_link_status}")


@pytest.mark.asyncio
async def test_replication_consistency(tmp_path: Path) -> None:
    master_port = unused_port()
    replica_port = unused_port()

    master_cfg = Config(
        host="127.0.0.1",
        port=master_port,
        dir=tmp_path / "master",
        aof_enabled=False,
        log_level="warning",
    )
    replica_cfg = Config(
        host="127.0.0.1",
        port=replica_port,
        dir=tmp_path / "replica",
        aof_enabled=False,
        role="replica",
        replicaof_host="127.0.0.1",
        replicaof_port=master_port,
        log_level="warning",
    )

    master = RedisServer(master_cfg)
    await master.start()

    # Seed master before replica connects
    mc = RespClient(master_cfg.host, master_cfg.port)
    await mc.connect()
    assert await mc.execute("SET", "pre", "seed") == "OK"
    assert await mc.execute("LPUSH", "L", "a") == 1
    assert await mc.execute("HSET", "H", "f", "1") == 1

    replica = RedisServer(replica_cfg)
    await replica.start()
    try:
        await _wait_replica_up(replica)

        rc = RespClient(replica_cfg.host, replica_cfg.port)
        await rc.connect()
        try:
            # A7.1 roles
            minfo = await mc.execute("INFO", "replication")
            rinfo = await rc.execute("INFO", "replication")
            assert isinstance(minfo, (bytes, bytearray))
            assert isinstance(rinfo, (bytes, bytearray))
            assert b"role:master" in minfo
            assert b"role:slave" in rinfo

            # A7.3 full sync data (RPOP is a write — verify list via store / read cmds)
            assert await rc.execute("GET", "pre") == b"seed"
            entry = replica.store.get_entry(b"L")
            assert entry is not None
            assert list(entry.value) == [b"a"]
            flat = await rc.execute("HGETALL", "H")
            assert isinstance(flat, list)
            mapping = dict(zip(flat[0::2], flat[1::2], strict=True))
            assert mapping == {b"f": b"1"}
            assert await rc.execute("DBSIZE") == 3

            # A7.4 incremental after sync
            assert await mc.execute("SET", "post", "incr") == "OK"
            deadline = asyncio.get_event_loop().time() + 2.0
            got = None
            while asyncio.get_event_loop().time() < deadline:
                got = await rc.execute("GET", "post")
                if got == b"incr":
                    break
                await asyncio.sleep(0.05)
            assert got == b"incr"

            # A7.5 readonly
            err = await rc.execute("SET", "x", "y")
            assert isinstance(err, ProtocolErrorMsg)
            assert "READONLY" in err.message
        finally:
            await rc.close()
    finally:
        await mc.close()
        await replica.stop()
        await master.stop()


@pytest.mark.asyncio
async def test_writes_during_fullresync_reach_replica(tmp_path: Path) -> None:
    """SET while FULLRESYNC is in progress must still appear on the replica.

    Uses master.full_resync_pause_hook to inject a write after the syncing
    link is registered (backlog path) and before RDB bytes are sent.
    """
    master_port = unused_port()
    replica_port = unused_port()

    master_cfg = Config(
        host="127.0.0.1",
        port=master_port,
        dir=tmp_path / "master-mid",
        aof_enabled=False,
        log_level="warning",
    )
    replica_cfg = Config(
        host="127.0.0.1",
        port=replica_port,
        dir=tmp_path / "replica-mid",
        aof_enabled=False,
        role="replica",
        replicaof_host="127.0.0.1",
        replicaof_port=master_port,
        log_level="warning",
    )

    master = RedisServer(master_cfg)
    await master.start()

    mc = RespClient(master_cfg.host, master_cfg.port)
    await mc.connect()
    assert await mc.execute("SET", "before", "1") == "OK"
    assert await mc.execute("LPUSH", "sync-list", "before") == 1

    pause_entered = asyncio.Event()
    resume = asyncio.Event()
    mid_write_done = asyncio.Event()

    async def pause_hook() -> None:
        pause_entered.set()
        await resume.wait()

    master.master.full_resync_pause_hook = pause_hook

    replica = RedisServer(replica_cfg)
    await replica.start()

    async def do_mid_write() -> None:
        await pause_entered.wait()
        # Concurrent write while replica is registered as syncing
        assert await mc.execute("SET", "during", "sync") == "OK"
        assert await mc.execute("LPUSH", "sync-list", "during") == 2
        mid_write_done.set()
        resume.set()

    writer_task = asyncio.create_task(do_mid_write())
    try:
        # Wait until pause hook has run (FULLRESYNC in progress) then mid-write
        await asyncio.wait_for(mid_write_done.wait(), timeout=5.0)
        await writer_task
        await _wait_replica_up(replica, timeout=5.0)

        rc = RespClient(replica_cfg.host, replica_cfg.port)
        await rc.connect()
        try:
            assert await rc.execute("GET", "before") == b"1"
            # Key written during FULLRESYNC must eventually be visible
            deadline = asyncio.get_event_loop().time() + 2.0
            got = None
            while asyncio.get_event_loop().time() < deadline:
                got = await rc.execute("GET", "during")
                if got == b"sync":
                    break
                await asyncio.sleep(0.05)
            assert got == b"sync"
            list_entry = replica.store.get_entry(b"sync-list")
            assert list_entry is not None
            assert list(list_entry.value) == [b"during", b"before"]
        finally:
            await rc.close()
    finally:
        resume.set()
        if not writer_task.done():
            writer_task.cancel()
            try:
                await writer_task
            except asyncio.CancelledError:
                pass
        await mc.close()
        await replica.stop()
        await master.stop()


@pytest.mark.asyncio
async def test_bgsave_rejects_concurrent(tmp_path: Path) -> None:
    """Second BGSAVE while first is in progress → ERR Background save already in progress."""
    port = unused_port()
    cfg = Config(
        host="127.0.0.1",
        port=port,
        dir=tmp_path / "bgsave",
        aof_enabled=False,
        log_level="warning",
    )
    server = RedisServer(cfg)
    await server.start()
    client = RespClient(cfg.host, cfg.port)
    await client.connect()
    try:
        assert await client.execute("SET", "k", "v") == "OK"

        gate = asyncio.Event()

        async def slow_bgsave_worker() -> None:
            async with server._bgsave_lock:
                await gate.wait()

        # Simulate an in-progress BGSAVE
        server._bgsave_task = asyncio.create_task(slow_bgsave_worker())
        err = await client.execute("BGSAVE")
        assert isinstance(err, ProtocolErrorMsg)
        assert "Background save already in progress" in err.message

        gate.set()
        await server._bgsave_task
    finally:
        await client.close()
        await server.stop()


def test_parser_max_buffer_bytes_raises() -> None:
    """Exceeding max_buffer_bytes raises ProtocolError."""
    parser = RespParser(max_bulk_len=1024, max_buffer_bytes=16)
    with pytest.raises(ProtocolError, match="max_buffer_bytes"):
        parser.feed(b"$100\r\n" + b"x" * 100 + b"\r\n")


@pytest.mark.asyncio
async def test_replica_rejects_snapshot_length_over_limit() -> None:
    """副本必须在接收快照主体前拒绝过大的 RDB 长度声明。"""
    reader = asyncio.StreamReader()
    reader.feed_data(b"$2048\r\n")
    parser = RespParser()

    async def apply_command(argv: list[bytes]) -> None:
        """测试占位回调；本用例不会执行复制命令。"""

    replica = ReplicaClient(
        "127.0.0.1",
        6379,
        Store(),
        apply_command,
        snapshot_max_bytes=1024,
    )
    with pytest.raises(ProtocolError, match="snapshot limit"):
        await replica._read_rdb_bulk(reader, parser)


@pytest.mark.asyncio
async def test_writes_during_fullresync_flush_window_reach_replica(
    tmp_path: Path,
) -> None:
    """SET during backlog flush/drain window must not be stranded after live.

    Uses full_resync_flush_hook to inject a write after a backlog batch is
    written but before drain — the flush-until-empty loop must pick it up
    before atomically switching to live (closes R3-BACKLOG-FLUSH-GAP).
    """
    master_port = unused_port()
    replica_port = unused_port()

    master_cfg = Config(
        host="127.0.0.1",
        port=master_port,
        dir=tmp_path / "master-flush",
        aof_enabled=False,
        log_level="warning",
    )
    replica_cfg = Config(
        host="127.0.0.1",
        port=replica_port,
        dir=tmp_path / "replica-flush",
        aof_enabled=False,
        role="replica",
        replicaof_host="127.0.0.1",
        replicaof_port=master_port,
        log_level="warning",
    )

    master = RedisServer(master_cfg)
    await master.start()

    mc = RespClient(master_cfg.host, master_cfg.port)
    await mc.connect()
    assert await mc.execute("SET", "seed", "1") == "OK"

    # Seed the syncing backlog before RDB send so the flush loop runs at least
    # once (otherwise flush_hook never fires).
    pause_entered = asyncio.Event()
    pause_resume = asyncio.Event()
    flush_entered = asyncio.Event()
    flush_resume = asyncio.Event()
    flush_write_done = asyncio.Event()

    async def pause_hook() -> None:
        pause_entered.set()
        await pause_resume.wait()

    async def flush_hook() -> None:
        # Only act on the first flush iteration
        if flush_entered.is_set():
            return
        flush_entered.set()
        await flush_resume.wait()

    master.master.full_resync_pause_hook = pause_hook
    master.master.full_resync_flush_hook = flush_hook

    replica = RedisServer(replica_cfg)
    await replica.start()

    async def inject_writes() -> None:
        await pause_entered.wait()
        # Land in backlog before RDB send (ensures flush loop has a batch)
        assert await mc.execute("SET", "preflush", "a") == "OK"
        pause_resume.set()
        await flush_entered.wait()
        # Land during flush window (after batch write, before/during drain await)
        assert await mc.execute("SET", "inflush", "window") == "OK"
        flush_write_done.set()
        flush_resume.set()

    inj = asyncio.create_task(inject_writes())
    try:
        await asyncio.wait_for(flush_write_done.wait(), timeout=5.0)
        await inj
        await _wait_replica_up(replica, timeout=5.0)

        rc = RespClient(replica_cfg.host, replica_cfg.port)
        await rc.connect()
        try:
            assert await rc.execute("GET", "seed") == b"1"
            assert await rc.execute("GET", "preflush") == b"a"
            deadline = asyncio.get_event_loop().time() + 2.0
            got = None
            while asyncio.get_event_loop().time() < deadline:
                got = await rc.execute("GET", "inflush")
                if got == b"window":
                    break
                await asyncio.sleep(0.05)
            assert got == b"window"
        finally:
            await rc.close()
    finally:
        pause_resume.set()
        flush_resume.set()
        if not inj.done():
            inj.cancel()
            try:
                await inj
            except asyncio.CancelledError:
                pass
        await mc.close()
        await replica.stop()
        await master.stop()


@pytest.mark.asyncio
async def test_replica_authenticates_to_password_protected_master(
    tmp_path: Path,
) -> None:
    """配置 masterauth 后，副本能够连接受 AUTH 保护的主节点。"""
    master_cfg = Config(
        host="127.0.0.1",
        port=unused_port(),
        dir=tmp_path / "auth-master",
        aof_enabled=False,
        requirepass="replication-secret",
        log_level="warning",
    )
    replica_cfg = Config(
        host="127.0.0.1",
        port=unused_port(),
        dir=tmp_path / "auth-replica",
        aof_enabled=False,
        role="replica",
        replicaof_host="127.0.0.1",
        replicaof_port=master_cfg.port,
        masterauth="replication-secret",
        log_level="warning",
    )
    master = RedisServer(master_cfg)
    replica = RedisServer(replica_cfg)
    await master.start()
    client = RespClient(master_cfg.host, master_cfg.port)
    await client.connect()
    try:
        assert await client.execute("AUTH", "replication-secret") == "OK"
        assert await client.execute("SET", "protected", "value") == "OK"
        await replica.start()
        await _wait_replica_up(replica)
        assert replica.store.get_string(b"protected") == b"value"
    finally:
        await client.close()
        await replica.stop()
        await master.stop()
