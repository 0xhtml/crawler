"""Utilities for working with URLs and pages."""

import math
import re

import fasttext
import httpx
import rfc3986
from httpx import URL
from lxml import etree, html
from lxml.html.clean import Cleaner

_BODY_XPATH = etree.XPath("//body")
_LANG_XPATH = etree.XPath("//*[@lang]")
_HREF_XPATH = etree.XPath("//a[not(@rel) or @rel!='nofollow']/@href")
_MODEL = fasttext.load_model("lid.176.bin")

HTML_CLEANER = Cleaner(
    style=True,
    links=False,
    meta=False,
    page_structure=False,
    remove_tags={"div", "span"},
    kill_tags={"noscript"},
    safe_attrs={
        "alt",
        "charset",
        "content",
        "href",
        "id",
        "lang",
        "media",
        "name",
        "property",
        "rel",
        "src",
        "target",
        "title",
        "type",
    },
)


def normalize_url(url: URL, history: list[httpx.Response] = []) -> URL:
    """
    Normalize a URL based on the RFC3986 spec.

    This function performs the following normalizations in addition to the
    normalization performed by httpx's URL:
        - Add trailing slash to URLs without path.
        - Remove trailing slash from all other URLs.
        - Remove any trailing '?' without query parameters.
        - Sort the query parameters.
    """
    return url.copy_with(
        path=url.path.rstrip("/") or "/",
        query=b"&".join(sorted(url.query.split(b"&"))) or None,
        fragment=None,
    )


def get_lang(dom: html.HtmlElement) -> str:
    """Detect the language of a html page."""
    langtags = _LANG_XPATH(dom)

    if langtags:
        return langtags[0].attrib.get("lang").split("-")[0].lower()

    body = (_BODY_XPATH(dom) or [dom])[0]

    text = " ".join(
        html.tostring(body, method="text", encoding="unicode").split()
    )
    start = max(0, math.floor(len(text) / 3) - 512)
    text = text[start : start + 1023]

    return _MODEL.predict(text)[0][0].replace("__label__", "")


def get_links(url: URL, dom: html.HtmlElement) -> set[URL]:
    """Get all links of a page."""
    links = set()

    for href in _HREF_XPATH(dom):
        try:
            try:
                parsed_href = url.join(href)
            except rfc3986.exceptions.ResolutionError:
                # FIXME this try-except shoudn't be required
                # https://github.com/encode/httpx/pull/2252
                parsed_href = url.copy_with(query=None).join(href)
        except httpx.InvalidURL as e:
            print(f"INVALID URL {href} on {str(url)[:80]}: {e}")
            continue

        if parsed_href.scheme not in ("http", "https"):
            continue

        links.add(normalize_url(parsed_href))

    return links
