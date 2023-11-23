"""Tests for the URL normalization."""

import pytest
from crawler.http import URL, InvalidURLError

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

INVALID_URLS = {
    ("ftp://foo.com", "scheme"),
    ("javascript:alert(1)", "scheme"),
    ("http://foo.com:123", "port"),
}

FROM_STRING_ERROR_URLS = {
    ("/foo.html", "scheme"),
    ("https:///foo.html", "host"),
    *INVALID_URLS,
}

JOIN_ERROR_URLS = {
    *(("https://foo.com/", url, field) for url, field in INVALID_URLS),
}

VALID_URLS = {
    "https://example.com/%2Afoo",
    *(pre for pre, post in NORMALIZE_URLS),
    *(base for base, url, joined in JOIN_URLS),
    *(joined for base, url, joined in JOIN_URLS),
    *(base for base, url, field in JOIN_ERROR_URLS),
}

FROM_STRING_URLS = {
    ("http://user@example.com/foo?a=b#c", "https://example.com/foo?a=b"),
    ("http://example.com/foo%2a", "https://example.com/foo%2A"),
    ("http://Example.COM/Foo", "https://example.com/Foo"),
    ("http://example.com/%7Efoo", "https://example.com/~foo"),
    ("http://example.com/%41foo", "https://example.com/Afoo"),
    ("http://example.com/foo/./b/baz/../", "https://example.com/foo/b/"),
    ("http://example.com", "https://example.com/"),
    ("http://example.com:443/", "https://example.com/"),
    ("http://example.com/display?", "https://example.com/display"),
    ("http://example.com/hällö", "https://example.com/h%C3%A4ll%C3%B6"),
    *((url, url) for url in VALID_URLS),
}


@pytest.mark.parametrize(("pre", "post"), FROM_STRING_URLS)
def test_url_from_string(pre: str, post: str):
    assert str(URL.from_string(pre)) == post


@pytest.mark.parametrize(("url", "field"), FROM_STRING_ERROR_URLS)
def test_url_from_string_error(url: str, field: str):
    with pytest.raises(InvalidURLError, match=field):
        URL.from_string(url)


@pytest.mark.parametrize(("pre", "post"), NORMALIZE_URLS)
def test_url_normalize(pre: str, post: str):
    assert str(URL.from_string(pre).normalize()) == post


@pytest.mark.parametrize(("base", "url", "joined"), JOIN_URLS)
def test_url_join(base: str, url: str, joined: str):
    assert str(URL.from_string(base).join(url)) == joined


@pytest.mark.parametrize(("base", "url", "field"), JOIN_ERROR_URLS)
def test_url_join_error(base: str, url: str, field: str):
    pre = URL.from_string(base)
    with pytest.raises(InvalidURLError, match=field):
        pre.join(url)


@pytest.mark.parametrize("url", [post for pre, post in FROM_STRING_URLS])
def test_url_to_httpx_url(url: str):
    pre = URL.from_string(url)
    assert str(pre) == str(pre.to_httpx_url())
