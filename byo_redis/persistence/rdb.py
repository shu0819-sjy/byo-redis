"""BYOR simplified RDB format (not wire-compatible with official Redis RDB).

Format (little-endian):
  magic: b'BYOR'
  version: uint32 = 1
  key_count: uint32
  repeated key records:
    key_len: uint32
    key: bytes
    type: uint8  (1=STRING, 2=LIST, 3=HASH)
    expire_at_ms: int64  (-1 if none)
    type-specific payload:
      STRING: value_len + value
      LIST:   count + (len+data)*count
      HASH:   count + (field_len+field + value_len+value)*count
  footer: b'EOF\\n'
"""

from __future__ import annotations

import logging
import os
import struct
from collections import deque
from pathlib import Path

from byo_redis.storage.expiry import is_expired, now_ms
from byo_redis.storage.store import Store
from byo_redis.storage.types import KeyEntry, RedisType

logger = logging.getLogger(__name__)

MAGIC = b"BYOR"
VERSION = 1
TYPE_STRING = 1
TYPE_LIST = 2
TYPE_HASH = 3
FOOTER = b"EOF\n"


class RdbError(Exception):
    """Corrupt or unsupported RDB file."""


def _pack_bytes(data: bytes) -> bytes:
    return struct.pack("<I", len(data)) + data


def _read_exact(buf: memoryview, offset: int, n: int) -> tuple[bytes, int]:
    end = offset + n
    if end > len(buf):
        raise RdbError("unexpected EOF while reading RDB")
    return bytes(buf[offset:end]), end


def _read_u32(buf: memoryview, offset: int) -> tuple[int, int]:
    raw, offset = _read_exact(buf, offset, 4)
    return struct.unpack("<I", raw)[0], offset


def _read_i64(buf: memoryview, offset: int) -> tuple[int, int]:
    raw, offset = _read_exact(buf, offset, 8)
    return struct.unpack("<q", raw)[0], offset


def _read_blob(buf: memoryview, offset: int) -> tuple[bytes, int]:
    length, offset = _read_u32(buf, offset)
    return _read_exact(buf, offset, length)


def dump_rdb_bytes(store: Store) -> bytes:
    entries = store.snapshot()
    parts: list[bytes] = [MAGIC, struct.pack("<I", VERSION), struct.pack("<I", len(entries))]
    for key, entry in entries.items():
        parts.append(_pack_bytes(key))
        if entry.type is RedisType.STRING:
            type_id = TYPE_STRING
        elif entry.type is RedisType.LIST:
            type_id = TYPE_LIST
        elif entry.type is RedisType.HASH:
            type_id = TYPE_HASH
        else:  # pragma: no cover
            raise RdbError(f"unknown type {entry.type}")
        parts.append(struct.pack("<B", type_id))
        expire = -1 if entry.expire_at_ms is None else int(entry.expire_at_ms)
        parts.append(struct.pack("<q", expire))
        if entry.type is RedisType.STRING:
            parts.append(_pack_bytes(entry.value))  # type: ignore[arg-type]
        elif entry.type is RedisType.LIST:
            items: deque[bytes] = entry.value  # type: ignore[assignment]
            parts.append(struct.pack("<I", len(items)))
            for item in items:
                parts.append(_pack_bytes(item))
        else:
            mapping: dict[bytes, bytes] = entry.value  # type: ignore[assignment]
            parts.append(struct.pack("<I", len(mapping)))
            for field, value in mapping.items():
                parts.append(_pack_bytes(field))
                parts.append(_pack_bytes(value))
    parts.append(FOOTER)
    return b"".join(parts)


def dump_rdb(store: Store, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dump_rdb_bytes(store)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "wb") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    logger.info("RDB saved to %s (%d bytes)", path, len(payload))


def load_rdb_bytes(data: bytes) -> dict[bytes, KeyEntry]:
    if len(data) < 12:
        raise RdbError("RDB too short")
    buf = memoryview(data)
    offset = 0
    magic, offset = _read_exact(buf, offset, 4)
    if magic != MAGIC:
        raise RdbError(f"bad RDB magic: {magic!r}")
    version, offset = _read_u32(buf, offset)
    if version != VERSION:
        raise RdbError(f"unsupported RDB version: {version}")
    count, offset = _read_u32(buf, offset)
    now = now_ms()
    entries: dict[bytes, KeyEntry] = {}
    for _ in range(count):
        key, offset = _read_blob(buf, offset)
        type_raw, offset = _read_exact(buf, offset, 1)
        type_id = type_raw[0]
        expire_at, offset = _read_i64(buf, offset)
        expire_at_ms = None if expire_at < 0 else expire_at
        if type_id == TYPE_STRING:
            value, offset = _read_blob(buf, offset)
            entry = KeyEntry(type=RedisType.STRING, value=value, expire_at_ms=expire_at_ms)
        elif type_id == TYPE_LIST:
            n, offset = _read_u32(buf, offset)
            items: deque[bytes] = deque()
            for _i in range(n):
                item, offset = _read_blob(buf, offset)
                items.append(item)
            entry = KeyEntry(type=RedisType.LIST, value=items, expire_at_ms=expire_at_ms)
        elif type_id == TYPE_HASH:
            n, offset = _read_u32(buf, offset)
            mapping: dict[bytes, bytes] = {}
            for _i in range(n):
                field, offset = _read_blob(buf, offset)
                value, offset = _read_blob(buf, offset)
                mapping[field] = value
            entry = KeyEntry(type=RedisType.HASH, value=mapping, expire_at_ms=expire_at_ms)
        else:
            raise RdbError(f"unknown type id {type_id}")
        if is_expired(entry.expire_at_ms, now=now):
            continue
        entries[key] = entry
    footer, offset = _read_exact(buf, offset, len(FOOTER))
    if footer != FOOTER:
        raise RdbError("missing RDB footer")
    return entries


def load_rdb(path: Path) -> dict[bytes, KeyEntry]:
    data = path.read_bytes()
    return load_rdb_bytes(data)


def load_rdb_into(store: Store, path: Path) -> int:
    entries = load_rdb(path)
    store.load_entries(entries)
    logger.info("RDB loaded from %s (%d keys)", path, len(entries))
    return len(entries)
