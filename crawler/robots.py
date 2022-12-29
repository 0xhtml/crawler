"""Module to deal with robots.txt files."""

import math
import time
import urllib.robotparser
from typing import Any, Optional

from httpx import URL

from .httpxclient import USER_AGENT, HTTPXClient


class RobotsFile(urllib.robotparser.RobotFileParser):
    """HTTPX AsyncClient supporting RobotFileParser."""

    async def load(self, client: HTTPXClient, url: URL):
        """Load robots.txt file."""
        response = await client.retrying_get(url)

        if response is not None and response.is_success:
            self.parse(response.text.splitlines())
        elif (
            response is not None
            and response.is_client_error
            and response.status_code != 429
        ):
            self.allow_all = True
        else:
            print(f"ROBOTS '{url}' dissallow_all")
            self.disallow_all = True


class RobotsFileTable:
    """A table to store robots.txt files for urls."""

    def __init__(self, client: HTTPXClient):
        """Initialize robots.txt file table."""
        self._client = client
        self._map: dict[bytes, RobotsFile] = {}

    def __getstate__(self) -> dict[str, Any]:
        """Return state used by pickle."""
        return {"_map": self._map}

    async def can_fetch(self, url: URL) -> bool:
        """Check if robot is allowed to fetch URL."""
        if (
            url.netloc not in self._map
            or self._map[url.netloc].mtime() + 24 * 60 * 60 < time.time()
        ):
            self._map[url.netloc] = RobotsFile()
            await self._map[url.netloc].load(
                self._client,
                url.copy_with(
                    userinfo=None,
                    path="/robots.txt",
                    query=None,
                    fragment=None,
                ),
            )

        return self._map[url.netloc].can_fetch(USER_AGENT, str(url))

    def timeout(self, url: URL) -> Optional[float]:
        """Get the timeout for the next request."""
        if url.netloc not in self._map:
            return None

        delay = self._map[url.netloc].crawl_delay(USER_AGENT)
        delay = 0 if delay is None else float(delay)

        rate = self._map[url.netloc].request_rate(USER_AGENT)
        rate = math.inf if rate is None else (rate.requests / rate.seconds)

        delay = max(delay, 1 / rate)
        if delay == 0:
            return None

        return time.time() + delay
