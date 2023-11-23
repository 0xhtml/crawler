"""Utilities for working with URLs and pages."""

import contextlib
import math

import fasttext
from lxml import etree, html
from lxml.html.clean import Cleaner

from .http import URL, InvalidURLError

_LANG_XPATH = etree.XPath("//*/@lang")
_BASE_XPATH = etree.XPath("//base/@href")
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


def get_lang(dom: html.HtmlElement) -> str:
    """Detect the language of a html page."""
    if (langtags := _LANG_XPATH(dom)):
        return langtags[0].split("-")[0].lower()

    text = " ".join(dom.body.text_content().split())
    start = max(0, math.floor(len(text) / 3) - 512)
    text = text[start : start + 1023]

    return _MODEL.predict(text)[0][0].removeprefix("__label__")


def get_links(url: URL, dom: html.HtmlElement) -> set[URL]:
    """Get all links of a page."""
    links = set()

    if (base := _BASE_XPATH(dom)):
        try:
            url = url.join(base[-1])
        except InvalidURLError:
            print("{url}: invalid base URL: {base[-1]}")
            return set()

    for href in _HREF_XPATH(dom):
        with contextlib.suppress(InvalidURLError):
            links.add(url.join(href))

    return links
