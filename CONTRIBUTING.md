# Contributing to byo-redis

Thanks for helping improve this educational Redis subset. Prefer clear module
boundaries and tests over feature sprawl.

## Development setup

Requirements: **Python 3.11+**.

```bash
git clone https://github.com/shu0819-sjy/byo-redis.git
cd byo-redis
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -e ".[dev]"
```

Run the server:

```bash
byo-redis --help
byo-redis --port 6379
# or: python -m byo_redis --port 6379
```

## Tests and gates

```bash
python -m compileall -q byo_redis
ruff check byo_redis tests
ruff format --check byo_redis tests
mypy byo_redis
python -m pytest -q
```

CI runs the same gates on Python 3.11 and 3.12. Fix failures locally before
opening a PR — do not widen `mypy` `strict` settings to silence new errors.

## Ground rules

1. **Keep the subset honest.** Document every new command in the README feature
   table and cover it with unit and/or integration tests.
2. **Module boundaries.** `protocol` encodes/decodes only; `commands` owns
   semantics; `storage` owns in-memory structures + TTL; persistence and
   replication hook through the server write path.
3. **No network in unit tests.** Integration tests may open local sockets; do
   not call the public internet.
4. **No secrets / PII.** Never commit real passwords, machine-absolute paths, or
   personal identity. Default bind stays localhost-oriented.
5. **English in new code comments** when practical; existing bilingual docs under
   `docs/` may stay as they are.

## Workflow

1. Branch from `main` (`feat/...`, `fix/...`, `docs/...`).
2. Update `CHANGELOG.md` for user-visible behavior.
3. Run the full local gate above until green.
4. Open a focused PR.

Commit messages: conventional, imperative subject ≤ 72 chars.

## Reporting issues

- Bugs: Python version, minimal RESP repro (`redis-cli` or test client), expected
  vs actual.
- Security issues: **do not** open a public issue — see [`SECURITY.md`](SECURITY.md).

## License

By contributing you agree that your contributions are licensed under the
[MIT License](LICENSE) of this repository.
