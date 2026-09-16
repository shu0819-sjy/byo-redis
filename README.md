# BYO-Redis

Build-Your-Own-Redis: an industrial-grade Redis protocol subset implemented with **Python asyncio**.

Compatible with `redis-cli` for the supported command set. Designed for correctness, clear module boundaries, persistence demos, and master/replica replication.

## Features (v0.1)

| Area | Commands / capability |
|------|------------------------|
| Protocol | RESP2 incremental parser, `asyncio.start_server` |
| String | `SET` / `GET` / `EXPIRE` / `TTL` |
| List | `LPUSH` / `RPOP` |
| Hash | `HSET` / `HGETALL` |
| Persistence | RDB snapshot (`SAVE` / `BGSAVE`) + AOF append/replay |
| Replication | Master → replica full sync (`PSYNC`) + write propagation |
| Ops | `PING` `ECHO` `INFO` `CONFIG GET` `DEL` `DBSIZE` `SELECT 0` |

> **RDB note:** snapshots use a documented **BYOR** binary format (`magic=BYOR`), not the official Redis RDB wire format. Recovery is self-consistent within BYO-Redis.

## Requirements

- Python **3.11+**
- Optional: [`redis-cli`](https://redis.io/docs/ui/cli/) for manual checks; automated tests use a pure-Python RESP client

## Install

```bash
cd byo-redis
python -m venv .venv
# Windows: .venv\Scripts\activate
# POSIX:  source .venv/bin/activate
pip install -e ".[dev]"
```

This installs the `byo-redis` console script and the `byo_redis` package.

## Start the server

```bash
# Master (default 127.0.0.1:6379, data in ./data)
python -m byo_redis --port 6379 --dir ./data
# or: byo-redis --port 6379 --dir ./data
```

### redis-cli / client examples

```bash
redis-cli -p 6379 PING
# PONG

redis-cli -p 6379 SET foo bar
redis-cli -p 6379 GET foo
# "bar"

redis-cli -p 6379 SET session:1 token EX 60
redis-cli -p 6379 TTL session:1
redis-cli -p 6379 EXPIRE foo 10

redis-cli -p 6379 LPUSH q a b c
redis-cli -p 6379 RPOP q

redis-cli -p 6379 HSET user:1 name Ada role admin
redis-cli -p 6379 HGETALL user:1
```

Without `redis-cli`, any RESP2 client (or the in-repo test helper) works the same over TCP.

## RDB and AOF

**Startup load order:** if AOF is enabled and the AOF file is non-empty → replay AOF; else if an RDB file exists → load RDB; else empty DB.

```bash
# Manual snapshot
redis-cli -p 6379 SAVE
# or non-blocking: BGSAVE

# Crash-recovery demo (RDB)
redis-cli -p 6379 SET durable 1
redis-cli -p 6379 SAVE
# kill the process, then restart with the same --dir → key restored

# AOF on (default) / off
python -m byo_redis --port 6379 --dir ./data --aof
python -m byo_redis --port 6379 --dir ./data --no-aof

# fsync policy: always | everysec | no
python -m byo_redis --aof-fsync everysec --dir ./data
```

### Persistence notes (v0.1)

- **AOF rewrite is not supported.** `appendonly.aof` can grow without bound under sustained writes.
- Operators should rotate/truncate the AOF with planned downtime (stop → archive/truncate → restart) or rely on periodic / manual **RDB snapshots** (`SAVE` / `BGSAVE`) and disable AOF when growth is a concern.
- AOF appends are queued and flushed by a background task so peer command latency is not blocked on disk `write`/`fsync`; `always` / `everysec` / `no` policies are preserved.

## Master / replica replication

```bash
# Terminal 1 — master
python -m byo_redis --port 6379 --dir ./data

# Terminal 2 — replica
python -m byo_redis --port 6380 --dir ./data-replica --replicaof 127.0.0.1:6379 --no-aof

# Seed on master, read on replica
redis-cli -p 6379 SET pre seed
redis-cli -p 6380 GET pre
# "seed"

redis-cli -p 6379 INFO replication
redis-cli -p 6380 INFO replication
```

Full sync uses `PSYNC` + RDB transfer with a syncing backlog so writes during the handshake are not lost. See [`docs/REPLICATION_NOTES.md`](docs/REPLICATION_NOTES.md).

## Configuration (CLI)

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `127.0.0.1` | Bind address |
| `--port` | `6379` | Listen port |
| `--dir` | `./data` | Data directory |
| `--dbfilename` | `dump.rdb` | RDB filename |
| `--aof` / `--no-aof` | AOF on | Enable/disable AOF |
| `--aof-filename` | `appendonly.aof` | AOF filename |
| `--aof-fsync` | `everysec` | `always` / `everysec` / `no` |
| `--replicaof HOST:PORT` | — | Run as replica |
| `--log-level` | `info` | Logging level |
| `--rdb-save-seconds` | `0` | Periodic BGSAVE interval (0 = off) |

## Tests

```bash
cd byo-redis
python -m compileall -q byo_redis
python -m pytest -q
```

Optional process-level E2E (force-kill RDB recovery, AOF replay, replication):

```bash
python docs/_qa_e2e_verify.py
```

CI runs compileall + pytest on Python 3.11/3.12 via [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

## Project layout

```text
byo-redis/
  byo_redis/          # server package (protocol, commands, storage, persistence, replication)
  tests/              # unit + integration
  docs/               # requirements, architecture, acceptance, verification, release notes
  pyproject.toml
  LICENSE
```

Specs and reports:

- [`docs/REQUIREMENTS.md`](docs/REQUIREMENTS.md)
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- [`docs/ACCEPTANCE.md`](docs/ACCEPTANCE.md)
- [`docs/VERIFICATION_REPORT.md`](docs/VERIFICATION_REPORT.md)
- [`docs/REPLICATION_NOTES.md`](docs/REPLICATION_NOTES.md)
- [`docs/RELEASE_CHECKLIST.md`](docs/RELEASE_CHECKLIST.md) — GitHub publish steps (push only after owner confirms repo name / visibility)

## Known limitations (v0.1)

- Subset of Redis commands only (see feature table); no pub/sub, transactions, streams, or Lua
- Single logical DB (`SELECT 0` only)
- BYOR RDB format (not Redis-compatible dump files)
- No AOF rewrite / compaction
- No `AUTH` / TLS — **do not expose to untrusted networks**
- Replication is async master→replica full sync + propagate (no diskless sync, no replica-of-replica tree tooling)
- Default bind is localhost

## Security

Default bind is `127.0.0.1`. There is **no AUTH/TLS** in v0.1 — treat this as a local learning / demo server unless you place it behind your own hardened network controls.

## License

MIT — see [`LICENSE`](LICENSE).
