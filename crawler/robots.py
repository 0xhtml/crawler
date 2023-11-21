"""Module to deal with robots.txt files."""

import math
import time
import urllib.robotparser

import httpx

from .http import URL, USER_AGENT, HTTPError, Netloc, Pool


class RobotsFile(urllib.robotparser.RobotFileParser):
    """Slim wrapper around urllib's RobotFileParser."""

    def can_fetch(self, url: URL) -> bool:
        """Check if robot can fetch the given URL."""
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

    async def _load(self, url: URL, pool: Pool) -> RobotsFile:
        robots_file = RobotsFile()
        robots_file.modified()

        try:
            response = await pool.get(url)
        except httpx.TooManyRedirects:
            robots_file.allow_all = True
            return robots_file
        except HTTPError as e:
            print(f"ROBOTS {url} {e.__class__.__name__}: {e}")
            robots_file.disallow_all = True
            return robots_file

        if response.is_success:
            robots_file.parse(response.text.splitlines())
        elif response.is_redirect:
            assert response.next_request is not None
            return await self._get(URL.from_httpx_url(response.next_request.url), pool)
        elif response.is_client_error and response.status_code != 429:
            robots_file.allow_all = True
        else:
            print(f"ROBOTS {url} dissallow_all (HTTP {response.status_code})")
            robots_file.disallow_all = True

        return robots_file

    async def _get(self, url: URL, pool: Pool) -> RobotsFile:
        if (
            url not in self._table
            or self._table[url].mtime() + 24 * 60 * 60 < time.time()
        ):
            self._table[url] = await self._load(url, pool)
        return self._table[url]

    async def get(self, netloc: Netloc, pool: Pool) -> RobotsFile:
        """Get the robots.txt for the given netloc."""
        url = URL(netloc.host, netloc.port, "/robots.txt", None)
        return await self._get(url, pool)
