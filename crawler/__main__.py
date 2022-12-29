"""Crawl the web."""

import asyncio
import signal
import pickle

from .crawler import Crawler


async def main():
    """Run main function."""
    try:
        with open("state.pkl", "rb") as file:
            crawler = pickle.load(file)
    except FileNotFoundError:
        crawler = Crawler()
        if not crawler.load_urls_db():
            crawler.add_url("https://en.wikipedia.org")

    async with crawler:
        workers = [asyncio.create_task(crawler.worker()) for _ in range(10)]

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(
            signal.SIGINT, lambda: asyncio.create_task(crawler.stop())
        )

        await asyncio.wait(workers)

    with open("state.pkl", "wb") as file:
        pickle.dump(crawler, file)


if __name__ == "__main__":
    asyncio.run(main())
