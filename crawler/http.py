"""HTTP client."""

import ssl
from typing import Any, NamedTuple, Optional
from urllib.parse import quote, unquote, urljoin, _UNSAFE_URL_BYTES_TO_REMOVE

import httpx

USER_AGENT = "crawler"


class InvalidURLError(Exception):
    """Raised when an invalid URL is encountered."""

    @classmethod
    def _check_str_not_empty(cls, url: httpx.URL, field: str) -> None:
        if (value := getattr(url, field)) == "":
            raise cls(field, value, "not empty")

    @classmethod
    def _check_in(cls, url: httpx.URL, field: str, expected: set[Any]) -> None:
        if (value := getattr(url, field)) not in expected:
            raise cls(field, value, f"in {expected!r}")

    @classmethod
    def check(cls, url: httpx.URL):
        """Check that the httpx URL is valid."""
        cls._check_in(url, "scheme", {"http", "https"})
        cls._check_str_not_empty(url, "host")
        cls._check_in(url, "port", {None, 80, 443})

    def __init__(self, field: str, value: object, expected: str) -> None:
        """Initialize the exception."""
        super().__init__(f"Invalid {field}: {value!r} expected {expected}")


class URL(NamedTuple):
    """Tuple representing an URL."""

    host: str
    path: str
    query: Optional[str]

    @classmethod
    def from_httpx_url(cls, url: httpx.URL) -> "URL":
        """Create a URL from a httpx URL."""
        InvalidURLError.check(url)

        return cls(
            url.host,
            quote(unquote(url.path)),
            url.query.decode() or None,
        )

    @classmethod
    def from_string(cls, url: str) -> "URL":
        """Create a URL from a string."""
        for b in _UNSAFE_URL_BYTES_TO_REMOVE:
            url = url.replace(b, "")

        try:
            return cls.from_httpx_url(httpx.URL(url))
        except httpx.InvalidURL as e:
            raise InvalidURLError("url", url, str(e)) from e

    def normalize(self) -> "URL":
        """
        Apply a few more extreme normalizations to the URL.

        - Remove trailing slashes
        - Sort query parameters
        """
        return self.__class__(
            self.host,
            self.path.rstrip("/") or "/",
            None if self.query is None else "&".join(sorted(self.query.split("&"))),
        )

    @property
    def target(self) -> str:
        """Target of the URL."""
        if self.query is None:
            return self.path
        return f"{self.path}?{self.query}"

    def join(self, url: str) -> "URL":
        """Join two URLs."""
        return self.from_string(urljoin(str(self), url))

    def __str__(self) -> str:
        """Convert the URL back to a string."""
        return f"https://{self.host}{self.target}"

    def to_httpx_url(self) -> httpx.URL:
        """Convert the URL to a httpx.URL."""
        return httpx.URL(
            scheme="https",
            host=self.host,
            raw_path=self.target.encode(),
        )


HTTPError = (
    httpx.NetworkError,
    httpx.ProtocolError,
    httpx.TimeoutException,
    httpx.TooManyRedirects,
    ssl.SSLError,
)


class Pool(httpx.AsyncClient):
    """Async connection pool with custom get method."""

    def __init__(self) -> None:
        """Initialize pool."""
        super().__init__(
            headers={"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"},
            timeout=httpx.Timeout(connect=4, write=1, read=10, pool=None),
        )

    async def get(
        self,
        url: URL,
        max_redirects: int = 5,
    ) -> httpx.Response:
        """Perform a GET request following redirects to same netloc."""
        request = httpx.Request("GET", url.to_httpx_url())

        while (response := await self.send(request)).is_redirect:
            assert response.next_request is not None

            max_redirects -= 1
            if max_redirects < 0:
                raise httpx.TooManyRedirects(
                    "Exceeded maximum allowed redirects.",
                    request=request,
                )

            InvalidURLError.check(response.next_request.url)
            if response.next_request.url.host != response.url.host:
                break

            request = response.next_request

        return response
