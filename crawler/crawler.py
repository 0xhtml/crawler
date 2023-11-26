"""The crawler that can crawl the internet."""

import asyncio
import contextlib
import logging
import re
import time
from typing import Any

import httpx
from lxml import etree, html

from . import Logger, db, logger
from .http import URL, HTTPError, Pool
from .robots import RobotsFileTable
from .utils import HTML_CLEANER, get_lang, get_links

_LANG_REGEX = re.compile(r"\b(?:en|de)\b", re.A | re.I)
_NOINDEX_REGEX = re.compile(r"\bnofollow\b", re.A | re.I)
_NOFOLLOW_REGEX = re.compile(r"\bnofollow\b", re.A | re.I)


def _check_headers(response: httpx.Response, log: Logger) -> bool:
    if not response.is_success:
        log.debug("not 2xx (HTTP %s)", response.status_code)
        return False

    content_type = response.headers.get("Content-Type", "")
    if not content_type.startswith("text/html"):
        log.info("not html (%s)", content_type)
        return False

    return True


def _index(response: httpx.Response, dom: html.HtmlElement, log: Logger) -> bool:
    robots = response.headers.get("X-Robots-Tag", "")
    if _NOINDEX_REGEX.search(robots):
        log.info("noindex (%s)", robots)
        return False

    lang = response.headers.get("Content-Language", "en")
    if not _LANG_REGEX.search(lang):
        log.debug("not en or de (%s)", lang)
        return False

    lang = get_lang(dom)
    if lang not in {"en", "de"}:
        log.debug("not en or de (%s)", lang)
        return False

    return True


def _follow(response: httpx.Response, log: Logger) -> bool:
    robots = response.headers.get("X-Robots-Tag", "")
    if _NOFOLLOW_REGEX.search(robots):
        log.info("nofollow (%s)", robots)
        return False

    return True


class Crawler:
    """The crawler."""

    def __getstate__(self) -> dict[str, Any]:
        """Return state used by pickle."""
        return {
            "_robots_file_table": self._robots_file_table,
            "_timeouts": self._timeouts,
            "_pending_urls": self._pending_urls,
            "_finished_urls": self._finished_urls,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restore state used by pickle."""
        self._pool = Pool()
        self._db = db.Session()
        self._stopping = False

        self.__dict__.update(state)

    def __init__(self) -> None:
        """Initialize the crawler w/ empty backlog and no connections."""
        self._robots_file_table = RobotsFileTable()
        self._timeouts: dict[str, float] = {}
        self._pending_urls: set[URL] = set()
        self._finished_urls: set[URL] = set()

        self.__setstate__({})

    async def __aenter__(self) -> "Crawler":
        """Call enter method of database connection."""
        await self._pool.__aenter__()
        self._db.__enter__()
        return self

    async def __aexit__(self, et, exc, tb) -> None:
        """Close database connection and all open http connections."""
        await self._pool.__aexit__(et, exc, tb)
        self._db.__exit__(et, exc, tb)

    def add_url(self, url: URL) -> None:
        """Add a URL to the pending URLs."""
        url = url.normalize()
        self._pending_urls.add(url)

    def stop(self) -> None:
        """Start stopping all workers."""
        logger.warning("Stopping...")
        self._stopping = True

    async def _load_page(self, url: URL) -> set[URL]:
        assert url not in self._finished_urls

        log = logging.LoggerAdapter(logger, {"url": url})

        robots_file = await self._robots_file_table.get(url.host, self._pool)

        if not robots_file.can_fetch(url):
            log.debug("disallowed by robots.txt")
            return set()

        try:
            response = await self._pool.get(url, True)
        except HTTPError as e:
            log.info("http error %s: %s", e.__class__.__name__, e)
            return set()
        finally:
            self._timeouts[url.host] = robots_file.timeout()

        if response.is_redirect:
            assert response.next_request is not None
            return {URL.from_httpx_url(response.next_request.url)}

        if not _check_headers(response, log):
            return set()

        try:
            dom = html.document_fromstring(response.content, ensure_head_body=True)
        except etree.ParserError as e:
            log.info("parser error %s: %s", e.__class__.__name__, e)
            return set()
        assert dom is not None
        HTML_CLEANER(dom)

        if _index(response, dom, log):
            self._db.merge(db.Document(url=str(url), content=html.tostring(dom)))
            self._db.commit()

        if not _follow(response, log):
            return set()

        return get_links(url, dom)

    async def run(self) -> None:
        """Run crawler."""
        tasks: dict[asyncio.Task, URL] = {}

        while not self._stopping:
            for url in self._pending_urls.copy():
                if len(tasks) >= 15:
                    break

                if time.time() < self._timeouts.get(url.host, 0):
                    continue

                if any(t.host == url.host for t in tasks.values()):
                    continue

                self._pending_urls.remove(url)
                tasks[asyncio.create_task(self._load_page(url))] = url

            done, _ = await asyncio.wait(
                tasks.keys(),
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                self._finished_urls.add(tasks.pop(task))

                urls = task.result()
                urls.difference_update(self._finished_urls)
                urls.difference_update(tasks.values())
                self._pending_urls.update(urls)

        for task, url in tasks.items():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self._pending_urls.add(url)

        self._stopping = False
