"""Module to deal with robots.txt files."""

import asyncio
import math
import time
import urllib.robotparser

import httpx

from .http import URL, USER_AGENT, HTTPError, InvalidURLError, Pool


class RobotsFile(urllib.robotparser.RobotFileParser):
    """Slim wrapper around urllib's RobotFileParser."""

    def __init__(self) -> None:
        """Initialize empty robots file."""
        self.lock = asyncio.Lock()
        super().__init__()
        self.modified()

    def parse(self, response: httpx.Response) -> None:
        """Parse the robots.txt file response."""
        if response.is_success:
            super().parse(response.text.splitlines())
        elif response.is_client_error and response.status_code != 429:
            self.allow_all = True
        else:
            self.disallow_all = True

    def expired(self) -> bool:
        """Return if the cached robots.txt should be reloaded."""
        assert self.mtime()
        return self.mtime() + 24 * 60 * 60 < time.time()

    def can_fetch(self, url: URL) -> bool:
        """Check if robot can fetch the given URL."""
        assert self.mtime()
        return super().can_fetch(USER_AGENT, str(url))

    def timeout(self) -> float:
        """Get the timeout for the next request."""
        assert self.mtime()

        delay = self.crawl_delay(USER_AGENT)
        delay = 0 if delay is None else float(delay)

        rate = self.request_rate(USER_AGENT)
        rate = math.inf if rate is None else (rate.requests / rate.seconds)

        return time.time() + max(delay, 1 / rate)


class RobotsFileTable:
    """Table of robots.txt files for all netlocs."""

    def __init__(self) -> None:
        """Initialize empty robots file table."""
        self._table: dict[URL, RobotsFile] = {}

    async def _get(self, url: URL, pool: Pool, max_redirects: int = 5) -> RobotsFile:
        assert max_redirects >= 0

        if url in self._table:
            async with self._table[url].lock:
                if not self._table[url].expired():
                    return self._table[url]

        self._table[url] = RobotsFile()

        async with self._table[url].lock:
            try:
                response = await pool.get(url, max_redirects == 0)
            except (InvalidURLError, httpx.TooManyRedirects):
                self._table[url].allow_all = True
                return self._table[url]
            except HTTPError as e:
                print(f"ROBOTS {url} {e.__class__.__name__}: {e}")
                self._table[url].disallow_all = True
                return self._table[url]

            if response.is_redirect:
                assert response.next_request is not None
                new_url = URL.from_httpx_url(response.next_request.url)
                self._table[url] = await self._get(new_url, pool, max_redirects - 1)
            else:
                self._table[url].parse(response)

            return self._table[url]

    async def get(self, host: str, pool: Pool) -> RobotsFile:
        """Get the robots.txt for the given netloc."""
        url = URL(host, "/robots.txt", None)
        return await self._get(url, pool)
