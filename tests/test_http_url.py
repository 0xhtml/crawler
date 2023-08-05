"""Tests for the URL normalization."""

from typing import Optional

import pytest
from crawler.http import URL, Netloc


@pytest.mark.parametrize(
    "url_string,netloc",
    [
        ("http://example.com/", Netloc("example.com", None)),
        ("http://user@example.com/", Netloc("example.com", None)),
        ("http://user:pass@example.com/", Netloc("example.com", None)),
        ("http://example.com:123/", Netloc("example.com", 123)),
        ("http://user@example.com:123/", Netloc("example.com", 123)),
        ("http://user:pass@example.com:123/", Netloc("example.com", 123)),
        ("http://example.com:443/", Netloc("example.com", None)),
        ("http://user@example.com:443/", Netloc("example.com", None)),
        ("http://user:pass@example.com:443/", Netloc("example.com", None)),
    ],
)
def test_url_netloc(url_string: str, netloc: Netloc):
    assert URL.from_string(url_string).netloc == netloc


@pytest.mark.parametrize(
    "pre,post",
    [
        ("http://user@example.com/foo?a=b#c", "https://example.com/foo?a=b"),
        ("http://example.com/foo%2a", "https://example.com/foo%2A"),
        ("http://Example.COM/Foo", "https://example.com/Foo"),
        ("http://example.com/%7Efoo", "https://example.com/~foo"),
        ("http://example.com/%41foo", "https://example.com/Afoo"),
        ("http://example.com/%2Afoo", "https://example.com/%2Afoo"),
        ("http://example.com/foo/./b/baz/../", "https://example.com/foo/b/"),
        ("http://example.com", "https://example.com/"),
        ("http://example.com:443/", "https://example.com/"),
        ("http://example.com:444/", "https://example.com:444/"),
        ("http://example.com/display?", "https://example.com/display"),
    ],
)
def test_url_normalization(pre: str, post: str):
    assert str(URL.from_string(pre)) == post


@pytest.mark.parametrize(
    "pre,post",
    [
        ("http://example.com/foo/", "https://example.com/foo"),
        (
            "http://example.com/display?lang=en&article=fred",
            "https://example.com/display?article=fred&lang=en",
        ),
    ],
)
def test_explicit_url_normalization(pre: str, post: str):
    assert str(URL.from_string(pre).normalize()) == post


@pytest.mark.parametrize(
    "base,url,joined",
    [
        ("http://foo.com/bar", "http://bar.com/foo", "https://bar.com/foo"),
        ("http://foo.com/bar", "//bar.com/foo", "https://bar.com/foo"),
        ("http://foo.com/bar/baz", "/foo", "https://foo.com/foo"),
        ("http://foo.com/foo/bar", "foo", "https://foo.com/foo/foo"),
        ("http://foo.com/foo/bar/", "foo", "https://foo.com/foo/bar/foo"),
        ("http://foo.com", "foo", "https://foo.com/foo"),
    ],
)
def test_url_join(base: str, url: str, joined: str):
    assert str(URL.from_string(base).join(url)) == joined
