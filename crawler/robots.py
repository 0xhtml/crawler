"""Module to deal with robots.txt files."""

import math
import time
import urllib.robotparser

from .httpxclient import USER_AGENT, HTTPXClient
from .http import URL, Netloc


class RobotsFile(urllib.robotparser.RobotFileParser):
    """HTTPX AsyncClient supporting RobotFileParser."""

    async def _load(self, netloc: Netloc, client: HTTPXClient):
        url = URL(netloc.host, netloc.port, "/robots.txt", None)
        response = await client.retrying_get(url.to_httpx_url())

        if response is not None and response.is_success:
            self.parse(response.text.splitlines())
        elif (
            response is not None
            and response.is_client_error
            and response.status_code != 429
        ):
            self.allow_all = True
            self.modified()
        else:
            print(f"ROBOTS '{url}' dissallow_all")
            self.disallow_all = True
            self.modified()

    async def can_fetch(self, url: URL, client: HTTPXClient) -> bool:
        """Check if robot is allowed to fetch URL."""
        if self.mtime() + 24 * 60 * 60 < time.time():
            await self._load(url.netloc, client)
        return super().can_fetch(USER_AGENT, str(url))

    def timeout(self) -> float:
        """Get the timeout for the next request."""
        assert self.mtime()

        delay = self.crawl_delay(USER_AGENT)
        delay = 0 if delay is None else float(delay)

        rate = self.request_rate(USER_AGENT)
        rate = math.inf if rate is None else (rate.requests / rate.seconds)

        return time.time() + max(delay, 1 / rate)
