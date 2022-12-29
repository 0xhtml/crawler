"""Tests for the URL normalization."""

from crawler.utils import normalize_url
from httpx import URL


def _check(pre: str, post: str, custom: bool = False):
    if custom:
        assert URL(pre) == pre
        assert normalize_url(URL(pre)) == post
    else:
        assert URL(pre) == post


def test_url_normilization():
    """
    Test the URL normalization.

    Based on https://en.wikipedia.org/wiki/URI_normalization
    """
    # Check for normalization
    _check("http://example.com/foo%2a", "http://example.com/foo%2A")
    _check("HTTP://User@Example.COM/Foo", "http://User@example.com/Foo")
    _check("http://example.com/%7Efoo", "http://example.com/~foo", True)
    _check("http://example.com/%41foo", "http://example.com/Afoo", True)
    _check("http://example.com/%2Afoo", "http://example.com/%2Afoo", True)
    _check(
        "http://example.com/foo/./bar/baz/../qux",
        "http://example.com/foo/bar/qux",
    )
    _check("http://example.com", "http://example.com/", True)
    _check("http://example.com:80/", "http://example.com/")
    _check("https://example.com:443/", "https://example.com/")
    _check("https://example.com/foo/", "https://example.com/foo", True)
    _check(
        "http://example.com/bar.html#section1",
        "http://example.com/bar.html",
        True,
    )
    _check(
        "http://example.com/display?lang=en&article=fred",
        "http://example.com/display?article=fred&lang=en",
        True,
    )
    _check("http://example.com/display?", "http://example.com/display", True)

    # Check that some URLs are still working
    _check(
        "https://web.archive.org/web/20220101000646/http://example.com",
        "https://web.archive.org/web/20220101000646/http%3A//example.com",
        True,
    )
