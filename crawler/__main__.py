"""Crawl the web."""

import asyncio
import pickle
import signal

from .crawler import Crawler
from .http import URL


async def main() -> None:
    """Run main function."""
    try:
        with open("state.pkl", "rb") as file:
            crawler = pickle.load(file)
    except FileNotFoundError:
        crawler = Crawler()
        crawler.add_url(URL.from_string("https://en.wikipedia.org"))

    asyncio.get_running_loop().add_signal_handler(signal.SIGINT, crawler.stop)
    async with crawler:
        await crawler.run()

    with open("state.pkl", "wb") as file:
        pickle.dump(crawler, file)


if __name__ == "__main__":
    asyncio.run(main())
