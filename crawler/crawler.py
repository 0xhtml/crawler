"""The crawler that can crawl the internet."""

import asyncio
import contextlib
import logging
import re
import time
from typing import Any

import httpx
import sqlalchemy
from lxml import html

from . import Logger, db, logger
from .http import URL, HTTPError, Pool
from .robots import RobotsFileTable
from .utils import HTML_CLEANER, get_lang, get_links

_LANG_REGEX = re.compile(r"\b(?:en|de)\b", re.A | re.I)
_NEWLINE_REGEX = re.compile(rb"\n+")
_NOFOLLOW_REGEX = re.compile(r"\bnofollow\b", re.A | re.I)


def _check_headers(response: httpx.Response, log: Logger) -> bool:
    if not response.is_success:
        log.debug("not 2xx (HTTP %s)", response.status_code)
        return False

    content_type = response.headers.get("Content-Type", "")
    if not content_type.startswith("text/html"):
        log.info("not html (%s)", content_type)
        return False

    robots = response.headers.get("X-Robots-Tag", "")
    if _NOFOLLOW_REGEX.search(robots):
        log.info("nofollow (%s)", robots)
        return False

    lang = response.headers.get("Content-Language", "en")
    if not _LANG_REGEX.search(lang):
        log.debug("not en or de (%s)", lang)
        return False

    return True


def _check_dom(dom: html.HtmlElement, log: Logger) -> bool:
    if dom is None:
        log.info("dom is None")
        return False

    HTML_CLEANER(dom)

    lang = get_lang(dom)
    if lang not in {"en", "de"}:
        log.debug("not en or de (%s)", lang)
        return False

    return True


class Crawler:
    """The crawler."""

    def __getstate__(self) -> dict[str, Any]:
        """Return state used by pickle."""
        return {
            "_robots_file_table": self._robots_file_table,
            "_timeouts": self._timeouts,
            "_urls": self._urls,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restore state used by pickle."""
        self._pool = Pool()
        self._db_conn = db.ENGINE.connect()
        self._stopping = False

        self.__dict__.update(state)

    def __init__(self) -> None:
        """Initialize the crawler w/ empty backlog and no connections."""
        self._robots_file_table = RobotsFileTable()
        self._timeouts: dict[str, float] = {}
        self._urls: set[URL] = set()

        self.__setstate__({})

    async def __aenter__(self) -> "Crawler":
        """Call enter method of database connection."""
        self._db_conn.__enter__()
        return self

    async def __aexit__(self, et, exc, tb) -> None:
        """Close database connection and all open http connections."""
        await self._pool.aclose()
        self._db_conn.__exit__(et, exc, tb)

    def add_url(self, url: URL) -> None:
        """Add a URL to the pending URLs."""
        url = url.normalize()
        self._urls.add(url)

    def stop(self) -> None:
        """Start stopping all workers."""
        logger.warning("Stopping...")
        self._stopping = True

    async def _load_page(self, url: URL) -> set[URL]:
        log = logging.LoggerAdapter(logger, {"url": url})

        if (
            self._db_conn.execute(
                sqlalchemy.select(db.DOCUMENTS_TABLE).filter_by(url=str(url)),
            ).first()
            is not None
        ):
            return set()

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

        dom = html.document_fromstring(response.content)
        if not _check_dom(dom, log):
            return set()

        self._db_conn.execute(
            sqlalchemy.insert(db.DOCUMENTS_TABLE).values(
                url=str(url),
                content=_NEWLINE_REGEX.sub(b"\n", html.tostring(dom)),
            ),
        )

        return get_links(url, dom)

    async def run(self) -> None:
        """Run crawler."""
        tasks: dict[asyncio.Task, URL] = {}

        while not self._stopping:
            for url in self._urls.copy():
                if len(tasks) >= 15:
                    break

                if time.time() < self._timeouts.get(url.host, 0):
                    continue

                if any(t.host == url.host for t in tasks.values()):
                    continue

                self._urls.remove(url)
                tasks[asyncio.create_task(self._load_page(url))] = url

            done, _ = await asyncio.wait(
                tasks.keys(),
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                tasks.pop(task)
                self._urls.update(task.result())

        for task, url in tasks.items():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self._urls.add(url)

        self._stopping = False
