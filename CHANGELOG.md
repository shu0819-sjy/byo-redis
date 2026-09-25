# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.3] — 2026-09-25

### Added

- CI hard gates: `ruff check`, `ruff format --check`, and `mypy byo_redis` (strict) alongside pytest
- Tag-driven GitHub Release workflow (`.github/workflows/release.yml`)

### Changed

- Applied `ruff format` across package and tests so the format gate stays green

## [0.2.2] — 2026-09-16

### Added

- Hardened Redis subset on asyncio: RESP parser/encoder, string/list/hash commands, TTL/expiry, AOF + RDB persistence, basic replication, memory limits / eviction hooks
- `byo-redis` console entry via `pip install -e .`
- Unit + integration test suite under `tests/`
- Architecture notes under `docs/`

## [0.1.0] — 2026-09-16

### Added

- Initial public release of the Build-Your-Own-Redis teaching/server subset
