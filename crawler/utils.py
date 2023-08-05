"""Utilities for working with URLs and pages."""

import math

import fasttext
from lxml import etree, html
from lxml.html.clean import Cleaner

from .http import URL, InvalidURLError

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
            links.add(url.join(href))
        except InvalidURLError:
            pass

    return links
