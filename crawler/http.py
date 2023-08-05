"""HTTP client."""

from typing import NamedTuple, Optional
from urllib.parse import quote, unquote

import httpx
import rfc3986
import rfc3986.normalizers


class InvalidURLError(Exception):
    """Raised when an invalid URL is encountered."""

    pass


class Netloc(NamedTuple):
    """Tuple representing a netloc."""

    host: str
    port: Optional[int]


class URL(NamedTuple):
    """Tuple representing an URL."""

    host: str
    port: Optional[int]
    path: str
    query: Optional[str]

    @classmethod
    def from_rfc3986_uri(cls, uri: rfc3986.URIReference) -> "URL":
        """Create a URL from a RFC 3986 URI."""
        uri = uri.normalize()

        assert isinstance(uri.scheme, str)
        assert uri.scheme in ["http", "https"]
        assert isinstance(uri.host, str)
        assert uri.host != ""
        assert isinstance(uri.port, Optional[str])
        assert uri.port != ""
        assert isinstance(uri.path, Optional[str])
        assert uri.path != ""
        assert isinstance(uri.query, Optional[str])

        return cls(
            uri.host,
            None if uri.port is None or uri.port == "443" else int(uri.port),
            "/" if uri.path is None else quote(unquote(uri.path)),
            uri.query or None,
        )

    @classmethod
    def from_httpx_url(cls, httpx_url: httpx.URL) -> "URL":
        return cls.from_rfc3986_uri(
            rfc3986.URIReference(
                httpx_url.scheme,
                httpx_url.netloc.decode(),
                httpx_url.path,
                httpx_url.query.decode(),
                None,
            )
        )

    @classmethod
    def from_string(cls, url_string: str) -> "URL":
        """Create a URL from a string."""
        return cls.from_rfc3986_uri(
            rfc3986.URIReference.from_string(url_string)
        )

    def normalize(self) -> "URL":
        """
        Apply a few more extreme normalizations to the URL.

        - Remove trailing slashes
        - Sort query parameters
        """
        return self.__class__(
            self.host,
            self.port,
            self.path.rstrip("/") or "/",
            None
            if self.query is None
            else "&".join(sorted(self.query.split("&"))),
        )

    @property
    def authority(self) -> str:
        """Authority of the URL."""
        if self.port is None:
            return self.host
        return f"{self.host}:{self.port}"

    @property
    def netloc(self) -> Netloc:
        """Netloc of the URL."""
        return Netloc(self.host, self.port)

    @property
    def target(self) -> str:
        """Target of the URL."""
        if self.query is None:
            return self.path
        return f"{self.path}?{self.query}"

    def join(self, url_string: str) -> "URL":
        """Join two URLs."""
        url = rfc3986.URIReference.from_string(url_string)

        assert isinstance(url.scheme, Optional[str])
        assert isinstance(url.host, Optional[str])
        assert url.host != ""
        assert isinstance(url.port, Optional[str])
        assert url.port != ""
        assert url.host is not None or url.port is None
        assert isinstance(url.path, Optional[str])
        assert url.path != ""
        assert isinstance(url.query, Optional[str])

        if url.scheme is not None:
            if url.scheme not in ["http", "https"] or url.host is None:
                raise InvalidURLError(url_string)
            return self.from_rfc3986_uri(url)

        if url.host is not None:
            return self.from_rfc3986_uri(url.copy_with(scheme="https"))

        if url.path is None:
            raise InvalidURLError(url_string)

        if url.path.startswith("/"):
            return self.from_rfc3986_uri(
                url.copy_with(scheme="https", authority=self.authority)
            )

        path = self.path or ""
        return self.from_rfc3986_uri(
            url.copy_with(
                scheme="https",
                authority=self.authority,
                path=f"{path[:path.rfind('/')]}/{url.path}",
            )
        )

    def __str__(self) -> str:
        """Convert the URL back to a string."""
        return f"https://{self.authority}{self.target}"

    def to_httpx_url(self) -> httpx.URL:
        """Convert the URL to a httpx URL."""
        return httpx.URL(
            scheme="https",
            host=self.host,
            port=self.port,
            raw_path=self.target.encode(),
        )
