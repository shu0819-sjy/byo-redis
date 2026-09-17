"""Unit tests for RESP parser and encoder."""

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
)
from byo_redis.protocol.parser import ProtocolError, RespParser


def test_simple_roundtrip() -> None:
    parser = RespParser()
    assert parser.feed(encode_simple_string("PONG")) == ["PONG"]


def test_bulk_and_null() -> None:
    parser = RespParser()
    assert parser.feed(encode_bulk_string(b"hello")) == [b"hello"]
    assert parser.feed(encode_null_bulk()) == [None]


def test_integer_and_error() -> None:
    parser = RespParser()
    assert parser.feed(encode_integer(42)) == [42]
    msgs = parser.feed(encode_error("ERR boom"))
    assert len(msgs) == 1
    assert msgs[0].message == "ERR boom"


def test_array_command() -> None:
    parser = RespParser()
    payload = encode_command([b"SET", b"k", b"v"])
    assert parser.feed(payload) == [[b"SET", b"k", b"v"]]


def test_sticky_packets() -> None:
    parser = RespParser()
    a = encode_command([b"PING"])
    b = encode_command([b"ECHO", b"x"])
    assert parser.feed(a + b) == [[b"PING"], [b"ECHO", b"x"]]


def test_partial_packets() -> None:
    parser = RespParser()
    payload = encode_command([b"SET", b"foo", b"bar"])
    mid = len(payload) // 2
    assert parser.feed(payload[:mid]) == []
    assert parser.feed(payload[mid:]) == [[b"SET", b"foo", b"bar"]]


def test_invalid_prefix() -> None:
    parser = RespParser()
    with pytest.raises(ProtocolError):
        parser.feed(b"?\r\n")


def test_bulk_limit() -> None:
    parser = RespParser(max_bulk_len=8)
    with pytest.raises(ProtocolError):
        parser.feed(b"$9\r\n123456789\r\n")


def test_array_element_limit() -> None:
    """数组元素数量超过上限时拒绝解析。"""
    parser = RespParser(max_array_len=2)
    with pytest.raises(ProtocolError, match="array length"):
        parser.feed(b"*3\r\n")


def test_encode_array_mixed() -> None:
    data = encode_array([b"a", 1, None])
    parser = RespParser()
    assert parser.feed(data) == [[b"a", 1, None]]
