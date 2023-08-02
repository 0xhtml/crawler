"""Module to deal with robots.txt files."""

import math
import time
import urllib.robotparser

from httpx import URL

from .httpxclient import USER_AGENT, HTTPXClient


class RobotsFile(urllib.robotparser.RobotFileParser):
    """HTTPX AsyncClient supporting RobotFileParser."""

    async def _load(self, url: URL, client: HTTPXClient):
        response = await client.retrying_get(url)

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
            await self._load(
                URL(scheme=url.scheme, netloc=url.netloc, path="/robots.txt"),
                client,
            )
        return super().can_fetch(USER_AGENT, str(url))

    def timeout(self, url: URL) -> float:
        """Get the timeout for the next request."""
        assert self.mtime()

        delay = self.crawl_delay(USER_AGENT)
        delay = 0 if delay is None else float(delay)

        rate = self.request_rate(USER_AGENT)
        rate = math.inf if rate is None else (rate.requests / rate.seconds)

        return time.time() + max(delay, 1 / rate)
