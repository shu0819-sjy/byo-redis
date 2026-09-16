"""RESP2 response encoders."""

from __future__ import annotations

from typing import Any


def encode_simple_string(value: str | bytes) -> bytes:
    if isinstance(value, str):
        value = value.encode("utf-8")
    if b"\r" in value or b"\n" in value:
        raise ValueError("simple string cannot contain CR or LF")
    return b"+" + value + b"\r\n"


def encode_error(message: str | bytes) -> bytes:
    if isinstance(message, str):
        message = message.encode("utf-8")
    message = message.replace(b"\r", b" ").replace(b"\n", b" ")
    return b"-" + message + b"\r\n"


def encode_integer(value: int) -> bytes:
    return f":{value:d}\r\n".encode("ascii")


def encode_bulk_string(value: str | bytes | None) -> bytes:
    if value is None:
        return encode_null_bulk()
    if isinstance(value, str):
        value = value.encode("utf-8")
    return f"${len(value)}\r\n".encode("ascii") + value + b"\r\n"


def encode_null_bulk() -> bytes:
    return b"$-1\r\n"


def encode_array(items: list[Any] | None) -> bytes:
    if items is None:
        return b"*-1\r\n"
    parts = [f"*{len(items)}\r\n".encode("ascii")]
    for item in items:
        parts.append(encode_value(item))
    return b"".join(parts)


def encode_value(value: Any) -> bytes:
    """Encode a Python value into RESP bytes.

    Supported:
      - None -> null bulk
      - bool is treated as int
      - int -> integer
      - bytes/bytearray/memoryview/str -> bulk string
      - list/tuple -> array
      - Exception-like objects with `.resp` bytes attribute are returned as-is
      - objects with method `to_resp()` returning bytes
    """
    if value is None:
        return encode_null_bulk()
    if isinstance(value, bool):
        return encode_integer(1 if value else 0)
    if isinstance(value, int):
        return encode_integer(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return encode_bulk_string(bytes(value))
    if isinstance(value, str):
        return encode_bulk_string(value)
    if isinstance(value, (list, tuple)):
        return encode_array(list(value))
    to_resp = getattr(value, "to_resp", None)
    if callable(to_resp):
        encoded = to_resp()
        if not isinstance(encoded, (bytes, bytearray)):
            raise TypeError("to_resp() must return bytes")
        return bytes(encoded)
    resp = getattr(value, "resp", None)
    if isinstance(resp, (bytes, bytearray)):
        return bytes(resp)
    raise TypeError(f"cannot encode type {type(value)!r} as RESP")


def encode_command(argv: list[bytes]) -> bytes:
    """Encode a command argv as a RESP array of bulk strings."""
    parts = [f"*{len(argv)}\r\n".encode("ascii")]
    for arg in argv:
        parts.append(encode_bulk_string(arg))
    return b"".join(parts)
