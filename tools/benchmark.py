"""BYO-Redis 轻量并发基准工具，不依赖第三方 Redis 客户端。"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time

from byo_redis.protocol.encoder import encode_command
from byo_redis.protocol.parser import ProtocolErrorMsg, RespParser


def percentile(values: list[float], ratio: float) -> float:
    """计算最近秩百分位；入参为非空数值列表和 0~1 比例，返回毫秒值。"""
    if not values:
        raise ValueError("values must not be empty")
    if not 0 <= ratio <= 1:
        raise ValueError("ratio must be between 0 and 1")
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(len(ordered) * ratio + 0.999999) - 1))
    return ordered[index]


def build_command(command: str, worker_id: int, index: int) -> list[bytes]:
    """生成单次基准命令；SET 使用唯一键，GET 读取预热键。"""
    if command == "ping":
        return [b"PING"]
    if command == "set":
        key = f"benchmark:{worker_id}:{index}".encode("ascii")
        return [b"SET", key, b"value"]
    if command == "get":
        return [b"GET", b"benchmark:shared"]
    raise ValueError(f"unsupported command: {command}")


async def read_reply(
    reader: asyncio.StreamReader, parser: RespParser
) -> object:
    """读取一条完整 RESP 响应；连接关闭时抛出 ConnectionError。"""
    while True:
        messages = parser.feed(b"")
        if messages:
            return messages[0]
        data = await reader.read(65_536)
        if not data:
            raise ConnectionError("server closed during benchmark")
        messages = parser.feed(data)
        if messages:
            return messages[0]


async def send_command(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    parser: RespParser,
    argv: list[bytes],
) -> object:
    """发送一条命令并返回响应；服务端错误会转换为 RuntimeError。"""
    writer.write(encode_command(argv))
    await writer.drain()
    reply = await read_reply(reader, parser)
    if isinstance(reply, ProtocolErrorMsg):
        raise RuntimeError(reply.message)
    return reply


async def run_worker(
    host: str,
    port: int,
    command: str,
    worker_id: int,
    request_count: int,
    password: str | None,
) -> list[float]:
    """运行一个连接上的顺序请求；返回每次往返延迟毫秒数。"""
    reader, writer = await asyncio.open_connection(host, port)
    parser = RespParser()
    latencies: list[float] = []
    try:
        if password is not None:
            await send_command(
                reader,
                writer,
                parser,
                [b"AUTH", password.encode("utf-8")],
            )
        for index in range(request_count):
            argv = build_command(command, worker_id, index)
            started = time.perf_counter_ns()
            await send_command(reader, writer, parser, argv)
            elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
            latencies.append(elapsed_ms)
    finally:
        writer.close()
        await writer.wait_closed()
    return latencies


async def prepare_get_key(
    host: str, port: int, password: str | None
) -> None:
    """为 GET 基准写入固定键；认证配置存在时先执行 AUTH。"""
    reader, writer = await asyncio.open_connection(host, port)
    parser = RespParser()
    try:
        if password is not None:
            await send_command(
                reader,
                writer,
                parser,
                [b"AUTH", password.encode("utf-8")],
            )
        await send_command(
            reader,
            writer,
            parser,
            [b"SET", b"benchmark:shared", b"value"],
        )
    finally:
        writer.close()
        await writer.wait_closed()


async def run_benchmark(args: argparse.Namespace) -> dict[str, object]:
    """执行并发基准；返回可序列化的吞吐和延迟结果。"""
    password = os.environ.get("BYO_REDIS_PASSWORD")
    if args.command == "get":
        await prepare_get_key(args.host, args.port, password)

    base = args.requests // args.concurrency
    remainder = args.requests % args.concurrency
    started = time.perf_counter()
    tasks = [
        asyncio.create_task(
            run_worker(
                args.host,
                args.port,
                args.command,
                worker_id,
                base + (1 if worker_id < remainder else 0),
                password,
            )
        )
        for worker_id in range(args.concurrency)
    ]
    batches = await asyncio.gather(*tasks)
    duration = time.perf_counter() - started
    latencies = [latency for batch in batches for latency in batch]
    return {
        "command": args.command.upper(),
        "requests": len(latencies),
        "concurrency": args.concurrency,
        "duration_seconds": round(duration, 6),
        "requests_per_second": round(len(latencies) / duration, 2),
        "latency_ms": {
            "min": round(min(latencies), 3),
            "p50": round(percentile(latencies, 0.50), 3),
            "p95": round(percentile(latencies, 0.95), 3),
            "p99": round(percentile(latencies, 0.99), 3),
            "max": round(max(latencies), 3),
        },
    }


def build_parser() -> argparse.ArgumentParser:
    """构建命令行参数解析器；所有参数均带有安全默认值和范围校验。"""
    parser = argparse.ArgumentParser(description="BYO-Redis RESP benchmark")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--requests", type=int, default=10_000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--command", choices=("ping", "set", "get"), default="ping")
    return parser


def main() -> None:
    """校验参数、运行基准并输出稳定 JSON。"""
    parser = build_parser()
    args = parser.parse_args()
    if not 1 <= args.port <= 65_535:
        parser.error("port must be between 1 and 65535")
    if args.requests <= 0:
        parser.error("requests must be > 0")
    if not 1 <= args.concurrency <= args.requests:
        parser.error("concurrency must be between 1 and requests")
    result = asyncio.run(run_benchmark(args))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
