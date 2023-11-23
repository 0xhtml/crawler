"""Tests for the URL normalization."""

import pytest
from crawler.http import URL, InvalidURLError

FROM_STRING_URLS = {
    ("http://user@example.com/foo?a=b#c", "https://example.com/foo?a=b"),
    ("http://example.com/foo%2a", "https://example.com/foo%2A"),
    ("http://Example.COM/Foo", "https://example.com/Foo"),
    ("http://example.com/%7Efoo", "https://example.com/~foo"),
    ("http://example.com/%41foo", "https://example.com/Afoo"),
    ("http://example.com/foo/./b/baz/../a", "https://example.com/foo/b/a"),
    ("http://example.com", "https://example.com/"),
    ("http://example.com:443/", "https://example.com/"),
    ("http://example.com/display?", "https://example.com/display"),
    ("http://example.com/hällö", "https://example.com/h%C3%A4ll%C3%B6"),
    ("http://example.com/f\to\ro\n.html", "https://example.com/foo.html"),
    ("https://example.com/%2Afoo", "https://example.com/%2Afoo"),
}

FROM_STRING_ERROR_URLS = {
    ("/foo.html", "scheme"),
    ("ftp://foo.com", "scheme"),
    ("javascript:alert(1)", "scheme"),
    ("https:///foo.html", "host"),
    ("http://foo.com:123", "port"),
}

NORMALIZE_URLS = {
    ("https://example.com/foo/", "https://example.com/foo"),
    (
        "https://example.com/display?lang=en&article=fred",
        "https://example.com/display?article=fred&lang=en",
    ),
}

JOIN_URLS = {
    ("https://foo.com/bar", "http://bar.com/foo", "https://bar.com/foo"),
    ("https://foo.com/bar", "//bar.com/foo", "https://bar.com/foo"),
    ("https://foo.com/bar/baz", "/foo", "https://foo.com/foo"),
    ("https://foo.com/foo/bar", "foo", "https://foo.com/foo/foo"),
    ("https://foo.com/foo/bar/", "foo", "https://foo.com/foo/bar/foo"),
    ("https://foo.com/", "foo", "https://foo.com/foo"),
    ("https://foo.com/bar", "?abc", "https://foo.com/bar?abc"),
}


@pytest.mark.parametrize(
    ("pre", "post"),
    FROM_STRING_URLS
    | {(pre, pre) for pre, post in NORMALIZE_URLS}
    | {(post, post) for post, pre in NORMALIZE_URLS}
    | {(base, base) for base, url, joined in JOIN_URLS}
    | {(joined, joined) for base, url, joined in JOIN_URLS},
)
def test_url_from_string(pre: str, post: str):
    assert str(URL.from_string(pre)) == post


@pytest.mark.parametrize(("url", "field"), FROM_STRING_ERROR_URLS)
def test_url_from_string_error(url: str, field: str):
    with pytest.raises(InvalidURLError, match=field):
        URL.from_string(url)


@pytest.mark.parametrize(
    ("pre", "post"),
    NORMALIZE_URLS
    | {(post, post) for pre, post in FROM_STRING_URLS}
    | {(joined, joined) for base, url, joined in JOIN_URLS},
)
def test_url_normalize(pre: str, post: str):
    assert str(URL.from_string(pre).normalize()) == post


@pytest.mark.parametrize(
    ("base", "url", "joined"),
    JOIN_URLS
    | {("https://foo.com", pre, post) for pre, post in FROM_STRING_URLS}
    | {("https://foo.com", post, post) for pre, post in FROM_STRING_URLS}
    | {("https://foo.com", pre, pre) for pre, post in NORMALIZE_URLS}
    | {("https://foo.com", post, post) for pre, post in NORMALIZE_URLS},
)
def test_url_join(base: str, url: str, joined: str):
    assert str(URL.from_string(base).join(url)) == joined


@pytest.mark.parametrize(
    "url",
    {post for pre, post in FROM_STRING_URLS}
    | {pre for pre, post in NORMALIZE_URLS}
    | {post for post, pre in NORMALIZE_URLS}
    | {base for base, url, joined in JOIN_URLS}
    | {joined for base, url, joined in JOIN_URLS},
)
def test_url_to_httpx_url(url: str):
    post = URL.from_string(url).to_httpx_url()
    assert url == str(post)
    assert url == str(URL.from_httpx_url(post))
