"""Expiry helpers using Unix epoch milliseconds."""

from __future__ import annotations

import time


def now_ms() -> int:
    return int(time.time() * 1000)


def is_expired(expire_at_ms: int | None, *, now: int | None = None) -> bool:
    if expire_at_ms is None:
        return False
    if now is None:
        now = now_ms()
    return now >= expire_at_ms


def ttl_seconds(expire_at_ms: int | None, *, now: int | None = None) -> int:
    """Redis TTL semantics: -1 no expire, -2 missing (caller), else remaining secs."""
    if expire_at_ms is None:
        return -1
    if now is None:
        now = now_ms()
    remaining_ms = expire_at_ms - now
    if remaining_ms <= 0:
        return -2
    # Redis rounds up partial seconds for TTL in many versions; use floor of secs left
    # but ensure at least 1 if still not expired.
    secs = remaining_ms // 1000
    return secs if secs > 0 else 0
