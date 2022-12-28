"""Crawl the web."""

import asyncio
import signal

from .crawler import Crawler


async def main():
    """Run main function."""
    async with Crawler() as crawler:
        if not crawler.load_urls_pkl("urls.pkl"):
            crawler.load_urls_db()

        workers = [asyncio.create_task(crawler.worker()) for _ in range(10)]

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(
            signal.SIGINT, lambda: asyncio.create_task(crawler.stop())
        )

        await asyncio.wait(workers)

        crawler.dump_urls_pkl("urls.pkl")


if __name__ == "__main__":
    asyncio.run(main())
