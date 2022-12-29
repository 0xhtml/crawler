"""Module containing HTTPXClient."""

import asyncio
from ssl import SSLError
from typing import Optional

import httpx

USER_AGENT = "crawler"


class HTTPXClient(httpx.AsyncClient):
    """Wrapper for httpx.AsyncClient."""

    _MAX_RETRIES = 2

    def __init__(self):
        """Set configuration for httpx.AsyncClient."""
        super().__init__(
            headers={"user-agent": USER_AGENT},
            http2=True,
            timeout=httpx.Timeout(connect=15, read=20, write=5, pool=None),
            follow_redirects=True,
        )

    async def retrying_request(
        self, m: str, url: httpx.URL
    ) -> Optional[httpx.Response]:
        """Call httpx's request retrying on errors."""
        for _ in range(self._MAX_RETRIES):
            try:
                return await self.request(m, url)
            except (
                httpx.NetworkError,
                httpx.ProtocolError,
                httpx.TimeoutException,
            ) as e:
                print(f"RETRY {m} {str(url)[:80]} {type(e).__name__} {e}")
                await asyncio.sleep(0.5)
            except (
                SSLError,
                UnicodeEncodeError,
                httpx.DecodingError,
                httpx.TooManyRedirects,
            ) as e:
                print(f"ERROR {m} {str(url)[:80]} {type(e).__name__} {e}")
                return None

        print(f"ERROR {m} {str(url)[:80]} too many tries")
        return None

    async def retrying_head(self, url: httpx.URL) -> Optional[httpx.Response]:
        """Call httpx's head retrying on errors."""
        return await self.retrying_request("HEAD", url)

    async def retrying_get(self, url: httpx.URL) -> Optional[httpx.Response]:
        """Call httpx's get retrying on errors."""
        return await self.retrying_request("GET", url)