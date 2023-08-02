"""The crawler that can crawl the internet."""

import asyncio
import re
import time
from typing import Any

import sqlalchemy
from httpx import URL
from lxml import etree, html
from tqdm import tqdm

from . import db
from .bucketset import BucketSet
from .httpxclient import HTTPXClient
from .robots import RobotsFile
from .utils import HTML_CLEANER, get_lang, get_links, normalize_url


class _SingleNetlocCrawler:
    _NEWLINE_REGEX = re.compile(rb"\n+")

    def __init__(self):
        self._robots_file = RobotsFile()
        self._timeout = 0

    def _exists(self, url: URL, db_conn):
        return (
            db_conn.execute(
                sqlalchemy.select(db.DOCUMENTS_TABLE).filter_by(url=str(url))
            ).first()
            is not None
        )

    async def _load_page(
        self, url: URL, client: HTTPXClient, db_conn
    ) -> set[URL]:
        if not await self._robots_file.can_fetch(url, client):
            return set()

        self._timeout = self._robots_file.timeout(url)

        response = await client.retrying_get(
            url, {"accept": "text/html", "accept-language": "de,en"}
        )
        if response is None:
            return set()

        if not response.is_success:
            print(f"SKIP {str(response.url)[:80]} HTTP {response.status_code}")
            return set()

        content_type = response.headers.get("content-type", "")
        if not content_type.startswith("text/html"):
            print(f"SKIP {str(response.url)[:80]} != html ({content_type})")
            return set()

        x_robots_tag = response.headers.get("x-robots-tag", "")
        if "nofollow" in x_robots_tag:
            print(
                f"SKIP {str(response.url)[:80]} nofollow "
                f"(X-Robots-Tag: {x_robots_tag})"
            )
            return set()

        if self._exists(response.url, db_conn):
            return set()

        dom = etree.fromstring(response.content, parser=html.html_parser)
        if dom is None:
            print(f"SKIP {str(response.url)[:80]} dom is None")
            return set()

        HTML_CLEANER(dom)

        if get_lang(dom) not in {"de", "en"}:
            print(f"SKIP {str(response.url)[:80]} lang isn't de or en")
            return set()

        db_conn.execute(
            sqlalchemy.insert(db.DOCUMENTS_TABLE).values(
                url=str(response.url),
                content=self._NEWLINE_REGEX.sub(b"\n", html.tostring(dom)),
            )
        )

        return get_links(response.url, dom)


class Crawler:
    """The crawler."""

    @staticmethod
    def _get_netloc(url: URL) -> bytes:
        return url.netloc

    def __getstate__(self) -> dict[str, Any]:
        """Return state used by pickle."""
        return {
            "_pending_urls": self._pending_urls,
            "_crawlers": self._crawlers,
        }

    def __setstate__(self, state: dict[str, Any]):
        """Restore state used by pickle."""
        self.__dict__.update(state)
        self._db_conn = db.ENGINE.connect()
        self._httpx_client = HTTPXClient()
        self._stopping = False

    def __init__(self):
        self.__setstate__({})
        self._pending_urls = BucketSet(self._get_netloc)
        self._crawlers: dict[bytes, _SingleNetlocCrawler] = {}

    async def __aenter__(self):
        """Call enter method of database connection and HTTPXClient."""
        self._db_conn.__enter__()
        await self._httpx_client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Close database connection and HTTPXClient."""
        self._db_conn.__exit__(exc_type, exc, tb)
        await self._httpx_client.__aexit__(exc_type, exc, tb)

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

    def stop(self):
        """Start stopping all workers."""
        print("Stopping...")
        self._stopping = True

    async def run(self):
        """Work for eternity or until stop is called."""
        tasks: dict[asyncio.Task, bytes] = {}

        try:
            while not self._stopping:
                for url in self._pending_urls.key_difference(tasks.values()):
                    if len(tasks) >= 1024:
                        break

                    netloc = self._get_netloc(url)

                    if netloc not in self._crawlers:
                        self._crawlers[netloc] = _SingleNetlocCrawler()
                    crawler = self._crawlers[netloc]

                    if crawler._timeout > time.time():
                        continue

                    task = asyncio.create_task(
                        crawler._load_page(
                            url, self._httpx_client, self._db_conn
                        )
                    )
                    tasks[task] = netloc

                done, _ = await asyncio.wait(
                    tasks.keys(), return_when=asyncio.FIRST_COMPLETED
                )

                for task in done:
                    tasks.pop(task)
                    self._pending_urls.update(task.result())
        finally:
            await asyncio.wait(tasks.keys(), return_when=asyncio.ALL_COMPLETED)
