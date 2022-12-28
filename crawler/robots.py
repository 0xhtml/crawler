"""Module to deal with robots.txt files."""

import urllib.robotparser

import httpx

from .httpxclient import USER_AGENT, HTTPXClient


class RobotsFile(urllib.robotparser.RobotFileParser):
    """HTTPX AsyncClient supporting RobotFileParser."""

    def __init__(self):
        """Initialize RobotsFile."""
        super().__init__()
        self._loaded = False

    async def load(self, client: HTTPXClient, url: httpx.URL):
        """Load robots.txt file."""
        if self._loaded:
            raise RuntimeError("Called load twice.")

        self._loaded = True

        response = await client.retrying_get(url)

        if response is not None and response.is_success:
            self.parse(response.text.splitlines())
        elif (
            response is not None
            and response.is_client_error
            and response.status_code not in (401, 403)
        ):
            self.allow_all = True
        else:
            print(f"ROBOTS '{url}' dissallow_all")
            self.disallow_all = True

    def can_fetch(self, url: httpx.URL) -> bool:
        """Check if robot is allowed to fetch URL."""
        if not self._loaded:
            raise RuntimeError("Called can_fetch w/o calling load before.")

        return super().can_fetch(USER_AGENT, str(url))


class RobotsFileTable:
    """A table to store robots.txt files for urls."""

    def __init__(self, client: HTTPXClient):
        """Initialize robots.txt file table."""
        self._client = client
        self._map: dict[httpx.URL, RobotsFile] = {}

    async def can_fetch(self, url: httpx.URL) -> bool:
        """Check if robot is allowed to fetch URL."""
        robot_url = url.copy_with(
            path="/robots.txt", query=None, fragment=None
        )

        if robot_url not in self._map:
            self._map[robot_url] = RobotsFile()
            await self._map[robot_url].load(self._client, robot_url)

        return self._map[robot_url].can_fetch(url)
