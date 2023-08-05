"""Crawl the web."""

import asyncio
import pickle
import signal

from .crawler import Crawler
from .http import URL


async def main():
    """Run main function."""
    try:
        with open("state.pkl", "rb") as file:
            crawler = pickle.load(file)
    except FileNotFoundError:
        crawler = Crawler()
        if not crawler.load_urls_db():
            crawler.add_url(URL.from_string("https://en.wikipedia.org"))

    try:
        async with crawler:
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGINT, crawler.stop)

            await crawler.run()
    finally:
        with open("state.pkl", "wb") as file:
            pickle.dump(crawler, file)


if __name__ == "__main__":
    asyncio.run(main())
