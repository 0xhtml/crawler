"""The crawler that can crawl the internet."""

import asyncio
import re
import time
from typing import NamedTuple

import httpx
import sqlalchemy
from httpx import URL
from lxml import etree, html
from tqdm import tqdm

from . import db
from .bucketset import BucketSet
from .httpxclient import HTTPXClient
from .robots import RobotsFileTable
from .utils import HTML_CLEANER, get_lang, get_links, normalize_url


def _is_valid_response(response: httpx.Response) -> bool:
    if not response.is_success:
        print(f"SKIP {str(response.url)[:80]} != 2xx ({response.status_code})")
        return False

    content_type = response.headers.get(
        "content-type",
        "text/html" if response.request.method == "HEAD" else "",
    )
    if not content_type.startswith("text/html"):
        print(f"SKIP {str(response.url)[:80]} != text/html ({content_type})")
        return False

    x_robots_tag = response.headers.get("x-robots-tag", "")
    if "nofollow" in x_robots_tag:
        print(f"SKIP {str(response.url)[:80]} nofollow (xrst: {x_robots_tag})")
        return False

    return True


class _CrawlerState(NamedTuple):
    robots_file_table: RobotsFileTable
    pending_urls: BucketSet[URL, bytes]
    timeouts: dict[bytes, float]


class Crawler:
    """The crawler."""

    _NEWLINE_REGEX = re.compile(rb"\n+")

    @staticmethod
    def _get_netloc(url: URL) -> bytes:
        return url.netloc

    def __init__(self):
        """Connect to database and init HTTPXClient."""
        self._db_conn = db.ENGINE.connect()
        self._httpx_client = HTTPXClient()
        self._robots_file_table = RobotsFileTable(self._httpx_client)

        self._pending_urls = BucketSet(self._get_netloc)
        self._active_urls: set[URL] = set()
        self._timeouts: dict[bytes, float] = {}

        self._stopping = False
        self._condition = asyncio.Condition()

    async def __aenter__(self):
        """Call enter method of database connection and HTTPXClient."""
        self._db_conn.__enter__()
        await self._httpx_client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Close database connection and HTTPXClient."""
        self._db_conn.__exit__(exc_type, exc, tb)
        await self._httpx_client.__aexit__(exc_type, exc, tb)

    def __getstate__(self) -> _CrawlerState:
        """Return state used by pickle."""
        if self._active_urls:
            raise RuntimeError("Can't get state of active crawler.")
        return _CrawlerState(
            self._robots_file_table, self._pending_urls, self._timeouts
        )

    def __setstate__(self, state: _CrawlerState):
        """Restore state loaded by pickle."""
        self.__init__()

        state.robots_file_table._client = self._httpx_client

        self._robots_file_table = state.robots_file_table
        self._pending_urls = state.pending_urls
        self._timeouts = state.timeouts

    def add_url(self, url: str):
        """Add a URL passed as str to the pending URLs."""
        self._pending_urls.add(normalize_url(URL(url)))

    def load_urls_db(self) -> bool:
        """Load the URLs out of the database (slow)."""
        count = self._db_conn.execute(
            sqlalchemy.func.count(db.DOCUMENTS_TABLE.c.url)
        ).scalar()
        if count is None or count <= 0:
            return False

        for document in tqdm(
            self._db_conn.execute(sqlalchemy.select(db.DOCUMENTS_TABLE)),
            total=count,
            ncols=100,
        ):
            dom = etree.fromstring(document.content, parser=html.html_parser)
            self._pending_urls.update(get_links(URL(document.url), dom))

        return True

    async def stop(self):
        """Start stopping all workers."""
        print("Stopping...")
        self._stopping = True
        async with self._condition:
            self._condition.notify_all()

    def _update_timeouts(self):
        ctime = time.time()
        self._timeouts = {
            netloc: timeout
            for netloc, timeout in self._timeouts.items()
            if timeout > ctime
        }

    def _exists(self, url: URL):
        return (
            self._db_conn.execute(
                sqlalchemy.select(db.DOCUMENTS_TABLE).filter_by(url=str(url))
            ).first()
            is not None
        )

    async def _load_page(self, url: URL) -> set[URL]:
        if not await self._robots_file_table.can_fetch(url):
            return set()

        head_response = await self._httpx_client.retrying_head(url)
        if (
            head_response is None
            or not _is_valid_response(head_response)
            or self._exists(head_response.url)
        ):
            return set()

        timeout = self._robots_file_table.timeout(url)
        if timeout is not None:
            self._timeouts[head_response.url.netloc] = timeout

        get_response = await self._httpx_client.retrying_get(head_response.url)
        if (
            get_response is None
            or not _is_valid_response(get_response)
            or self._exists(get_response.url)
        ):
            return set()

        dom = etree.fromstring(get_response.content, parser=html.html_parser)
        if dom is None:
            print(f"SKIP {str(get_response.url)[:80]} dom is None")
            return set()

        HTML_CLEANER(dom)

        if get_lang(dom) not in {"de", "en"}:
            print(f"SKIP {str(get_response.url)[:80]} lang isn't de or en")
            return set()

        self._db_conn.execute(
            sqlalchemy.insert(db.DOCUMENTS_TABLE).values(
                url=str(get_response.url),
                content=self._NEWLINE_REGEX.sub(b"\n", html.tostring(dom)),
            )
        )

        return get_links(get_response.url, dom)

    async def worker(self):
        """Work for eternity or until stop is called."""
        while not self._stopping:
            while True:
                self._update_timeouts()

                netlocs = {url.netloc for url in self._active_urls}
                urls = self._pending_urls.key_difference(
                    netlocs.union(self._timeouts.keys())
                )
                if urls:
                    break

                async with self._condition:
                    await self._condition.wait()
                if self._stopping:
                    return

            url = urls.pop()
            self._pending_urls.remove(url)
            self._active_urls.add(url)

            self._pending_urls.update(
                (await self._load_page(url)).difference(self._active_urls)
            )

            self._active_urls.remove(url)

            async with self._condition:
                self._condition.notify_all()
