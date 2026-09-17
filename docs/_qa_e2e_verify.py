"""QA E2E verification harness (temporary). Run from byo-redis/: python docs/_qa_e2e_verify.py"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from byo_redis.protocol.encoder import encode_command  # noqa: E402
from byo_redis.protocol.parser import ProtocolErrorMsg, RespParser  # noqa: E402


def unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class RespClient:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._parser = RespParser()

    async def connect(self, retries: int = 50) -> None:
        last: Exception | None = None
        for _ in range(retries):
            try:
                self._reader, self._writer = await asyncio.open_connection(
                    self.host, self.port
                )
                return
            except OSError as e:
                last = e
                await asyncio.sleep(0.05)
        raise ConnectionError(f"connect failed: {last}")

    async def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass

    async def execute(self, *parts: str | bytes | int) -> object:
        argv: list[bytes] = []
        for p in parts:
            if isinstance(p, bytes):
                argv.append(p)
            elif isinstance(p, str):
                argv.append(p.encode())
            else:
                argv.append(str(p).encode())
        assert self._writer and self._reader
        self._writer.write(encode_command(argv))
        await self._writer.drain()
        while True:
            if self._parser.buffered_size:
                msgs = self._parser.feed(b"")
                if msgs:
                    return msgs[0]
            data = await self._reader.read(65536)
            if not data:
                raise ConnectionError("server closed")
            msgs = self._parser.feed(data)
            if msgs:
                return msgs[0]


def pair_mapping(value: object) -> dict[object, object]:
    """将交替键值列表转为字典；入参必须是偶数长度列表，返回键值映射，异常类型直接断言失败。"""
    assert isinstance(value, list)
    assert len(value) % 2 == 0
    return dict(zip(value[0::2], value[1::2], strict=True))


def start_server(
    port: int, data_dir: Path, extra: list[str] | None = None
) -> subprocess.Popen[bytes]:
    data_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "byo_redis",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--dir",
        str(data_dir),
        "--log-level",
        "warning",
    ]
    if extra:
        cmd.extend(extra)
    return subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )


def force_kill(proc: subprocess.Popen[bytes]) -> None:
    if os.name == "nt":
        # Hard kill — simulates crash (no graceful stop)
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
            capture_output=True,
            check=False,
        )
    else:
        proc.kill()
    try:
        proc.wait(timeout=5)
    except Exception:
        pass


async def section_commands(port: int) -> list[str]:
    evidence: list[str] = []
    c = RespClient("127.0.0.1", port)
    await c.connect()
    try:
        assert await c.execute("PING") == "PONG"
        evidence.append("PING → PONG")

        assert await c.execute("SET", "foo", "bar") == "OK"
        assert await c.execute("GET", "foo") == b"bar"
        evidence.append("SET/GET foo=bar OK")

        assert await c.execute("SET", "exk", "v", "EX", "1") == "OK"
        await asyncio.sleep(1.15)
        assert await c.execute("GET", "exk") is None
        evidence.append("SET EX 1 expired → null")

        assert await c.execute("SET", "ek", "v") == "OK"
        assert await c.execute("EXPIRE", "ek", 1) == 1
        await asyncio.sleep(1.15)
        assert await c.execute("GET", "ek") is None
        evidence.append("EXPIRE then GET null")

        assert await c.execute("LPUSH", "mylist", "a") == 1
        assert await c.execute("LPUSH", "mylist", "b") == 2
        assert await c.execute("RPOP", "mylist") == b"a"
        assert await c.execute("RPOP", "mylist") == b"b"
        assert await c.execute("RPOP", "mylist") is None
        evidence.append("LPUSH/RPOP order a then b then null")

        assert await c.execute("HSET", "hk", "f1", "v1") == 1
        assert await c.execute("HSET", "hk", "f1", "v2") == 0
        flat = await c.execute("HGETALL", "hk")
        mapping = pair_mapping(flat)
        assert mapping == {b"f1": b"v2"}
        evidence.append("HSET/HGETALL f1=v2")

        assert await c.execute("LPUSH", "wt", "z") == 1
        err = await c.execute("GET", "wt")
        assert isinstance(err, ProtocolErrorMsg) and "WRONGTYPE" in err.message
        evidence.append("WRONGTYPE on GET list")
    finally:
        await c.close()
    return evidence


async def section_rdb_crash(tmp: Path) -> list[str]:
    evidence: list[str] = []
    port = unused_port()
    data = tmp / "rdb_crash"
    proc = start_server(port, data, ["--no-aof"])
    try:
        c = RespClient("127.0.0.1", port)
        await c.connect()
        assert await c.execute("SET", "str", "hello") == "OK"
        assert await c.execute("LPUSH", "lst", "a", "b") == 2
        assert await c.execute("HSET", "hs", "f", "v") == 1
        assert await c.execute("SET", "ttlkey", "t", "EX", "3600") == "OK"
        assert await c.execute("SAVE") == "OK"
        rdb = data / "dump.rdb"
        assert rdb.exists() and rdb.stat().st_size > 0
        evidence.append(f"SAVE ok; RDB size={rdb.stat().st_size}")
        await c.close()

        force_kill(proc)
        evidence.append(f"force-killed PID {proc.pid} (taskkill /F)")

        # Restart same dir
        proc2 = start_server(port, data, ["--no-aof"])
        try:
            c2 = RespClient("127.0.0.1", port)
            await c2.connect()
            assert await c2.execute("GET", "str") == b"hello"
            assert await c2.execute("RPOP", "lst") == b"a"
            flat = await c2.execute("HGETALL", "hs")
            mapping = pair_mapping(flat)
            assert mapping == {b"f": b"v"}
            assert await c2.execute("GET", "ttlkey") == b"t"
            evidence.append("post-crash restart restored str/list/hash/ttlkey")
            await c2.close()
        finally:
            force_kill(proc2)
    finally:
        if proc.poll() is None:
            force_kill(proc)
    return evidence


async def section_aof(tmp: Path) -> list[str]:
    evidence: list[str] = []
    port = unused_port()
    data = tmp / "aof_e2e"
    proc = start_server(
        port, data, ["--aof", "--aof-fsync", "always"]
    )
    try:
        c = RespClient("127.0.0.1", port)
        await c.connect()
        assert await c.execute("SET", "a", "1") == "OK"
        assert await c.execute("LPUSH", "L", "x") == 1
        assert await c.execute("HSET", "H", "f", "v") == 1
        await c.execute("GET", "a")
        aof = data / "appendonly.aof"
        assert aof.exists() and aof.stat().st_size > 0
        content = aof.read_bytes()
        assert b"SET" in content and b"GET" not in content
        evidence.append(f"AOF size={aof.stat().st_size}; contains SET; no GET")
        await c.close()
        force_kill(proc)

        # Prefer AOF: remove RDB if present
        rdb = data / "dump.rdb"
        if rdb.exists():
            rdb.unlink()

        proc2 = start_server(port, data, ["--aof", "--aof-fsync", "always"])
        try:
            c2 = RespClient("127.0.0.1", port)
            await c2.connect()
            assert await c2.execute("GET", "a") == b"1"
            assert await c2.execute("RPOP", "L") == b"x"
            flat = await c2.execute("HGETALL", "H")
            mapping = pair_mapping(flat)
            assert mapping == {b"f": b"v"}
            evidence.append("AOF replay restored a/L/H after restart (RDB removed)")
            await c2.close()
        finally:
            force_kill(proc2)
    finally:
        if proc.poll() is None:
            force_kill(proc)
    return evidence


async def section_replication(tmp: Path) -> list[str]:
    evidence: list[str] = []
    mport = unused_port()
    rport = unused_port()
    master = start_server(mport, tmp / "repl_m", ["--no-aof"])
    try:
        mc = RespClient("127.0.0.1", mport)
        await mc.connect()
        assert await mc.execute("SET", "pre", "seed") == "OK"
        assert await mc.execute("LPUSH", "L", "a") == 1
        assert await mc.execute("HSET", "H", "f", "1") == 1

        replica = start_server(
            rport,
            tmp / "repl_r",
            ["--no-aof", "--replicaof", f"127.0.0.1:{mport}"],
        )
        try:
            rc = RespClient("127.0.0.1", rport)
            await rc.connect()
            # wait for sync
            deadline = time.monotonic() + 5.0
            got = None
            while time.monotonic() < deadline:
                got = await rc.execute("GET", "pre")
                if got == b"seed":
                    break
                await asyncio.sleep(0.05)
            assert got == b"seed"
            evidence.append("full sync: replica GET pre=seed")

            minfo = await mc.execute("INFO", "replication")
            rinfo = await rc.execute("INFO", "replication")
            assert isinstance(minfo, bytes) and b"role:master" in minfo
            assert isinstance(rinfo, bytes) and b"role:slave" in rinfo
            evidence.append("INFO role:master / role:slave")

            flat = await rc.execute("HGETALL", "H")
            mapping = pair_mapping(flat)
            assert mapping == {b"f": b"1"}
            evidence.append("replica HGETALL H matches")

            assert await mc.execute("SET", "post", "incr") == "OK"
            deadline = time.monotonic() + 2.0
            got = None
            while time.monotonic() < deadline:
                got = await rc.execute("GET", "post")
                if got == b"incr":
                    break
                await asyncio.sleep(0.05)
            assert got == b"incr"
            evidence.append("incremental SET post=incr visible ≤2s")

            err = await rc.execute("SET", "x", "y")
            assert isinstance(err, ProtocolErrorMsg) and "READONLY" in err.message
            evidence.append("replica SET → READONLY")
            await rc.close()
        finally:
            force_kill(replica)
        await mc.close()
    finally:
        force_kill(master)
    return evidence


async def main() -> int:
    tmp = ROOT / "_qa_tmp"
    if tmp.exists():
        import shutil

        shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True, exist_ok=True)

    results: dict[str, list[str]] = {}
    failures: list[str] = []

    # Live server for command matrix
    port = unused_port()
    proc = start_server(port, tmp / "cmd", ["--no-aof"])
    try:
        results["commands"] = await section_commands(port)
        print("PASS commands:", "; ".join(results["commands"]))
    except Exception as e:
        failures.append(f"commands: {e!r}")
        print("FAIL commands:", e)
    finally:
        force_kill(proc)

    try:
        results["rdb_crash"] = await section_rdb_crash(tmp)
        print("PASS rdb_crash:", "; ".join(results["rdb_crash"]))
    except Exception as e:
        failures.append(f"rdb_crash: {e!r}")
        print("FAIL rdb_crash:", e)

    try:
        results["aof"] = await section_aof(tmp)
        print("PASS aof:", "; ".join(results["aof"]))
    except Exception as e:
        failures.append(f"aof: {e!r}")
        print("FAIL aof:", e)

    try:
        results["replication"] = await section_replication(tmp)
        print("PASS replication:", "; ".join(results["replication"]))
    except Exception as e:
        failures.append(f"replication: {e!r}")
        print("FAIL replication:", e)

    print("---")
    if failures:
        print("OVERALL FAIL")
        for f in failures:
            print(" ", f)
        return 1
    print("OVERALL PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
