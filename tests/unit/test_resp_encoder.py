"""Focused unit tests for RESP encoder edge cases."""

from __future__ import annotations

import pytest

from byo_redis.protocol.encoder import (
    encode_array,
    encode_bulk_string,
    encode_command,
    encode_error,
    encode_integer,
    encode_null_bulk,
    encode_simple_string,
    encode_value,
)


def test_simple_string_rejects_crlf() -> None:
    with pytest.raises(ValueError, match="CR or LF"):
        encode_simple_string("bad\r")
    with pytest.raises(ValueError, match="CR or LF"):
        encode_simple_string(b"bad\n")


def test_error_sanitizes_embedded_newlines() -> None:
    assert encode_error("ERR line1\rline2\nend") == b"-ERR line1 line2 end\r\n"


def test_bulk_none_is_null() -> None:
    assert encode_bulk_string(None) == encode_null_bulk() == b"$-1\r\n"


def test_bulk_str_and_bytes() -> None:
    assert encode_bulk_string("hi") == b"$2\r\nhi\r\n"
    assert encode_bulk_string(b"hi") == b"$2\r\nhi\r\n"


def test_array_null_and_empty() -> None:
    assert encode_array(None) == b"*-1\r\n"
    assert encode_array([]) == b"*0\r\n"


def test_encode_value_bool_as_int() -> None:
    assert encode_value(True) == encode_integer(1)
    assert encode_value(False) == encode_integer(0)


def test_encode_value_memoryview_and_bytearray() -> None:
    assert encode_value(memoryview(b"ab")) == b"$2\r\nab\r\n"
    assert encode_value(bytearray(b"x")) == b"$1\r\nx\r\n"


def test_encode_value_to_resp_hook() -> None:
    class Box:
        def to_resp(self) -> bytes:
            return b"+BOX\r\n"

    assert encode_value(Box()) == b"+BOX\r\n"


def test_encode_value_resp_attr() -> None:
    class Err:
        resp = b"-ERR custom\r\n"

    assert encode_value(Err()) == b"-ERR custom\r\n"


def test_encode_value_to_resp_must_return_bytes() -> None:
    class Bad:
        def to_resp(self) -> str:
            return "nope"

    with pytest.raises(TypeError, match="to_resp"):
        encode_value(Bad())


def test_encode_value_unsupported_type() -> None:
    with pytest.raises(TypeError, match="cannot encode"):
        encode_value(object())


def test_encode_command_argv() -> None:
    assert encode_command([b"PING"]) == b"*1\r\n$4\r\nPING\r\n"
