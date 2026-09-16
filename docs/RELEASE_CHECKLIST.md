# Release checklist (v0.1 → GitHub)

Use this before the first public push. **Do not force-push** and do not create a remote until the owner confirms repository name and visibility.

## Gate status (must be green)

| Gate | Status | Evidence |
|------|--------|----------|
| Requirements (t1) | PASS | `docs/REQUIREMENTS.md`, `docs/ARCHITECTURE.md`, `docs/ACCEPTANCE.md` |
| Implementation + repairs (t2/t6/t8/t10) | PASS | `byo_redis/`, pytest 38 |
| Verification (t3) | PASS | `docs/VERIFICATION_REPORT.md` |
| Review r4 (t11) | PASS | FULLRESYNC flush-until-empty; no high/blocker findings |

## Pre-publish hygiene

- [ ] Working tree clean of secrets / credentials / personal tokens (none expected in v0.1)
- [ ] Runtime dirs ignored: `data/`, `*.rdb`, `*.aof`, `.venv/`, `__pycache__/`, `.pytest_cache/`
- [ ] `LICENSE` present (MIT)
- [ ] `README.md` covers install, start, client examples, RDB/AOF, replication, tests, known limits
- [ ] `pyproject.toml` version matches package (`0.1.0`)
- [ ] Local sanity:
  ```bash
  cd byo-redis
  python -m compileall -q byo_redis
  python -m pytest -q
  ```
- [ ] Optional E2E: `python docs/_qa_e2e_verify.py`

## Recommended repository settings

| Item | Recommendation |
|------|----------------|
| Repository name | `byo-redis` (or `build-your-own-redis-asyncio`) |
| Visibility | **Public** (educational / open-source demo) |
| Default branch | `main` |
| Topics | `redis`, `asyncio`, `python`, `resp`, `rdb`, `aof`, `replication` |
| License | MIT (already in tree) |

Confirm name + visibility with the owner before `git remote add` / `git push`.

## Recommended first-push commands

Run from the `byo-redis/` project root after confirmation:

```bash
# 1) Initialize (skip if already a git repo)
git init -b main

# 2) Stage only publishable content
git add LICENSE README.md pyproject.toml .gitignore \
  byo_redis tests docs .github
git status   # review: no data/, no .venv/, no secrets

# 3) First commit
git commit -m "chore: release BYO-Redis v0.1.0 (asyncio Redis subset)"

# 4) Create remote (GitHub CLI example — replace OWNER/REPO after confirmation)
# gh repo create OWNER/byo-redis --public --source=. --remote=origin --push

# Or classic remote + push:
# git remote add origin git@github.com:OWNER/byo-redis.git
# git push -u origin main
```

### Tagging a release (optional)

```bash
git tag -a v0.1.0 -m "BYO-Redis v0.1.0"
git push origin v0.1.0
# gh release create v0.1.0 --title "v0.1.0" --notes-file docs/VERIFICATION_REPORT.md
```

## What not to push

- `data/`, `data-*/`, `*.rdb`, `*.aof`
- `.venv/`, `__pycache__/`, `.pytest_cache/`, `.env`
- Local IDE settings (already gitignored)

## Post-push smoke

1. Clone fresh into a temp dir
2. `pip install -e ".[dev]"`
3. `python -m byo_redis --port 6379 --dir ./data`
4. `redis-cli -p 6379 PING` (or run `python -m pytest -q`)
