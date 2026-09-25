# BYO-Redis

[![CI](https://github.com/shu0819-sjy/byo-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/shu0819-sjy/byo-redis/actions/workflows/ci.yml)

Build-Your-Own-Redis: a hardened, educational Redis protocol subset implemented with **Python asyncio**.

Compatible with `redis-cli` for the supported command set. Designed for correctness, clear module boundaries, persistence demos, and master/replica replication.

## Features (v0.2.3)

| Area | Commands / capability |
|------|------------------------|
| Protocol | RESP2 incremental parser, `asyncio.start_server` |
| String | `SET` / `GET` / `EXPIRE` / `TTL` |
| List | `LPUSH` / `RPOP` |
| Hash | `HSET` / `HGETALL` |
| Persistence | Checksummed RDB snapshot (`SAVE` / `BGSAVE`) + AOF append/replay/rewrite (`BGREWRITEAOF`) |
| Replication | Master → replica full sync (`PSYNC` + `REPLCONF`) + write propagation |
| Ops | `PING` `ECHO` `INFO` `CONFIG GET` `DEL` `DBSIZE` `SELECT 0` `COMMAND` |
| Security | `AUTH`, protected non-loopback binding, bounded client/protocol queues |

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

### Authentication and remote binding

```bash
# Prefer an environment variable so the password is not stored in shell history.
# PowerShell:
$env:BYO_REDIS_PASSWORD='replace-with-a-secret'
python -m byo_redis --host 0.0.0.0

redis-cli -a replace-with-a-secret PING
```

Without a configured password, BYO-Redis refuses non-loopback binds by default. The `--allow-unprotected-non-loopback` override is intentionally unsafe and should only be used in an isolated test network. TLS is not built in.

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

### Persistence notes (v0.2.3)

- `BGREWRITEAOF` compacts the current keyspace into a new AOF and atomically replaces the old file. Writes pause behind the server write barrier during the rewrite; reads remain available.
- Operators should monitor AOF size and schedule rewrites before storage pressure becomes critical.
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
| `--requirepass` | — | Require `AUTH`; prefer `BYO_REDIS_PASSWORD` to avoid shell history |
| `--masterauth` | — | Password used by a replica for its master |
| `--max-clients` | `1000` | Maximum simultaneous clients |
| `--max-buffer-bytes` | `32000000` | Per-connection RESP input buffer limit |
| `--proto-max-bulk-len` | `16777216` | Maximum RESP bulk-string length |
| `--proto-max-array-len` | `1024` | Maximum RESP array element count |
| `--max-array-depth` | `16` | Maximum nested RESP array depth |
| `--client-idle-timeout-sec` | `300` | Disconnect idle clients after this many seconds |
| `--max-auth-failures` | `5` | Close a connection after repeated failed `AUTH` attempts |
| `--aof-queue-max-commands` | `10000` | Pending AOF command limit |
| `--replica-backlog-max-bytes` | `67108864` | Full-sync replica backlog limit |
| `--persistence-max-load-bytes` | `1073741824` | Maximum RDB/AOF bytes accepted during load/full sync |
| `--maxmemory-bytes` | `0` | Approximate keyspace memory limit; `0` disables it |
| `--maxmemory-policy` | `noeviction` | `noeviction`, `allkeys-lru`, or `volatile-ttl` |

## Tests

```bash
cd byo-redis
python -m compileall -q byo_redis
python -m ruff check byo_redis tests tools docs/_qa_e2e_verify.py
python -m mypy byo_redis tools docs/_qa_e2e_verify.py
python -m pytest -q
```

Optional process-level E2E (force-kill RDB recovery, AOF replay, replication):

```bash
python docs/_qa_e2e_verify.py
```

CI runs compileall + pytest plus a 75% coverage gate on Python 3.11/3.12 via [`.github/workflows/ci.yml`](.github/workflows/ci.yml); the current suite reports 78% branch coverage.

Run the zero-dependency local benchmark against a started server:

```bash
python -m tools.benchmark --requests 10000 --concurrency 50 --command ping
```

See [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md) for the measured v0.2.1 baseline and interpretation limits.

### Durability and resource limits

- `--aof-fsync always` does not acknowledge a write until the AOF record has been written and fsynced; `everysec` acknowledges after the write queue accepts and persists the record, with periodic fsync.
- AOF queue, replica backlog, RESP bulk/array/depth, per-connection input buffer, and total client connections are bounded.
- Local persistence loading and replica full-sync snapshots are rejected before allocation when they exceed `--persistence-max-load-bytes`.
- `maxmemory` supports rollback-safe `noeviction`, approximate `allkeys-lru`, and `volatile-ttl`; evictions are recorded in AOF and replication.
- Idle clients and connections with repeated failed `AUTH` attempts are closed. This is per-connection protection, not a replacement for a network firewall or global source-IP rate limiting.
- `INFO` exposes uptime, accepted/rejected connections, processed/failed command counters, keyspace, replication, and persistence health metrics.
- AOF or fsync failures return `MISCONF` for the affected write instead of silently reporting success.
- BGSAVE and replica snapshot encoding run outside the asyncio event-loop thread after taking a consistent in-memory snapshot.

## Project layout

```text
byo-redis/
  byo_redis/          # server package (protocol, commands, storage, persistence, replication)
  tests/              # unit + integration
  tools/              # zero-dependency benchmark tooling
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
- [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md)
- [`docs/RELEASE_CHECKLIST.md`](docs/RELEASE_CHECKLIST.md) — GitHub publish steps (push only after owner confirms repo name / visibility)

## Known limitations (v0.2.3)

- Subset of Redis commands only (see feature table); no pub/sub, transactions, streams, or Lua
- Single logical DB (`SELECT 0` only)
- BYOR RDB format (not Redis-compatible dump files)
- Single-password `AUTH` only; no ACL users or TLS
- Replication is async master→replica full sync + propagate (no diskless sync, no replica-of-replica tree tooling)
- Default bind is localhost

This release is still not a drop-in production Redis replacement: it has no ACL users or TLS, no bounded-memory eviction policy, no partial PSYNC backlog recovery, and only one logical database. Expose it only behind an encrypted network boundary.

## Security

Default bind is `127.0.0.1`. A non-loopback bind is rejected unless `AUTH` is configured or the unsafe override is explicitly supplied. TLS is not implemented, so remote deployment still requires an encrypted network boundary.

Vulnerability reporting (private): [`SECURITY.md`](SECURITY.md).

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for local gates (`ruff` / `mypy` / `pytest`) and PR expectations.

## License

MIT — see [`LICENSE`](LICENSE).
