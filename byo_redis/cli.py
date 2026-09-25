"""CLI entrypoint for BYO-Redis."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from byo_redis.config import config_from_args
from byo_redis.server import RedisServer


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def _run(argv: list[str] | None = None) -> int:
    config = config_from_args(argv)
    _setup_logging(config.log_level)
    server = RedisServer(config)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            # Windows 的部分事件循环不支持 Unix signal handler，保留 Ctrl+C 回退。
            continue
    serve_task = asyncio.create_task(server.serve_forever(), name="redis-serve")
    stop_task = asyncio.create_task(stop_event.wait(), name="redis-stop-wait")
    try:
        done, _ = await asyncio.wait({serve_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if serve_task in done:
            await serve_task
    except asyncio.CancelledError:
        raise
    finally:
        stop_task.cancel()
        try:
            await stop_task
        except asyncio.CancelledError:
            pass
        serve_task.cancel()
        try:
            await serve_task
        except asyncio.CancelledError:
            pass
        await server.stop()
    return 0


def main(argv: list[str] | None = None) -> None:
    try:
        raise SystemExit(asyncio.run(_run(argv)))
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Interrupted")
        raise SystemExit(0) from None


if __name__ == "__main__":
    main(sys.argv[1:])
