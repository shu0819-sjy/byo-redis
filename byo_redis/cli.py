"""CLI entrypoint for BYO-Redis."""

from __future__ import annotations

import asyncio
import logging
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
    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        pass
    finally:
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
