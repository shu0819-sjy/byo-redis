"""Incremental RESP2 parser (handles sticky / partial TCP packets)."""

from __future__ import annotations

from typing import Any


class ProtocolError(Exception):
    """Raised when the byte stream violates RESP framing rules."""


class RespParser:
    """Feed-based RESP2 parser.

    Call :meth:`feed` with incoming bytes; it returns zero or more complete
    top-level RESP values. Residual bytes stay buffered for the next feed.

    ``max_buffer_bytes`` caps residual+incoming buffer size; exceeding it
    raises :class:`ProtocolError` so the server can close the connection.
    """

    def __init__(
        self,
        max_bulk_len: int = 16_777_216,
        max_buffer_bytes: int = 32_000_000,
    ) -> None:
        self._buf = bytearray()
        self._max_bulk_len = max_bulk_len
        self.max_buffer_bytes = max_buffer_bytes

    def feed(self, data: bytes) -> list[Any]:
        if data:
            if len(self._buf) + len(data) > self.max_buffer_bytes:
                raise ProtocolError(
                    f"input buffer exceeds max_buffer_bytes "
                    f"({self.max_buffer_bytes})"
                )
            self._buf.extend(data)
        elif len(self._buf) > self.max_buffer_bytes:
            raise ProtocolError(
                f"input buffer exceeds max_buffer_bytes ({self.max_buffer_bytes})"
            )
        messages: list[Any] = []
        while True:
            value, consumed = self._try_parse(0)
            if consumed == 0:
                break
            del self._buf[:consumed]
            messages.append(value)
        return messages

    def reset(self) -> None:
        self._buf.clear()

    @property
    def buffered_size(self) -> int:
        return len(self._buf)

    def _try_parse(self, start: int) -> tuple[Any, int]:
        if start >= len(self._buf):
            return None, 0
        prefix = self._buf[start]
        if prefix == ord("+"):
            return self._parse_line_value(start, simple=True)
        if prefix == ord("-"):
            line, end = self._read_line(start + 1)
            if end == 0:
                return None, 0
            return ProtocolErrorMsg(line.decode("utf-8", errors="replace")), end
        if prefix == ord(":"):
            line, end = self._read_line(start + 1)
            if end == 0:
                return None, 0
            try:
                return int(line), end
            except ValueError as exc:
                raise ProtocolError(f"invalid integer: {line!r}") from exc
        if prefix == ord("$"):
            return self._parse_bulk(start)
        if prefix == ord("*"):
            return self._parse_array(start)
        raise ProtocolError(f"invalid RESP type prefix: {chr(prefix)!r}")

    def _parse_line_value(self, start: int, *, simple: bool) -> tuple[Any, int]:
        line, end = self._read_line(start + 1)
        if end == 0:
            return None, 0
        text = line.decode("utf-8", errors="replace")
        return text, end

    def _parse_bulk(self, start: int) -> tuple[Any, int]:
        line, hdr_end = self._read_line(start + 1)
        if hdr_end == 0:
            return None, 0
        try:
            length = int(line)
        except ValueError as exc:
            raise ProtocolError(f"invalid bulk length: {line!r}") from exc
        if length == -1:
            return None, hdr_end
        if length < -1:
            raise ProtocolError(f"invalid bulk length: {length}")
        if length > self._max_bulk_len:
            raise ProtocolError(f"bulk length {length} exceeds limit")
        total_needed = hdr_end + length + 2
        if len(self._buf) < total_needed:
            return None, 0
        data = bytes(self._buf[hdr_end : hdr_end + length])
        crlf = self._buf[hdr_end + length : hdr_end + length + 2]
        if crlf != b"\r\n":
            raise ProtocolError("bulk string missing CRLF trailer")
        return data, total_needed

    def _parse_array(self, start: int) -> tuple[Any, int]:
        line, hdr_end = self._read_line(start + 1)
        if hdr_end == 0:
            return None, 0
        try:
            count = int(line)
        except ValueError as exc:
            raise ProtocolError(f"invalid array length: {line!r}") from exc
        if count == -1:
            return None, hdr_end
        if count < -1:
            raise ProtocolError(f"invalid array length: {count}")
        if count > 1_000_000:
            raise ProtocolError(f"array length {count} exceeds limit")
        items: list[Any] = []
        cursor = hdr_end
        for _ in range(count):
            item, end = self._try_parse(cursor)
            if end == 0:
                return None, 0
            items.append(item)
            cursor = end
        return items, cursor

    def _read_line(self, start: int) -> tuple[bytes, int]:
        """Return (line_without_crlf, absolute_end_index) or ("", 0) if incomplete."""
        idx = self._buf.find(b"\r\n", start)
        if idx < 0:
            return b"", 0
        return bytes(self._buf[start:idx]), idx + 2


class ProtocolErrorMsg:
    """Parsed RESP error value (not raised)."""

    __slots__ = ("message",)

    def __init__(self, message: str) -> None:
        self.message = message

    def __repr__(self) -> str:
        return f"ProtocolErrorMsg({self.message!r})"
