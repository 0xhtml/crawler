"""The crawler that can crawl the internet."""

import asyncio
import re
import time
from typing import Any

import sqlalchemy
from lxml import etree, html
from tqdm import tqdm

from . import db
from .httpxclient import HTTPXClient
from .http import URL, Netloc
from .robots import RobotsFile
from .utils import HTML_CLEANER, get_lang, get_links


class _SingleNetlocCrawler:
    _NEWLINE_REGEX = re.compile(rb"\n+")

    def __init__(self, netloc: Netloc):
        self._netloc = netloc
        self._robots_file = RobotsFile()
        self._pending_urls: set[URL] = set()
        self._timeout = 0

    def add_url(self, url: URL):
        self._pending_urls.add(url)

    def ready(self) -> bool:
        return bool(self._pending_urls) and self._timeout < time.time()

    def _exists(self, url: str, db_conn):
        return (
            db_conn.execute(
                sqlalchemy.select(db.DOCUMENTS_TABLE).filter_by(url=str(url))
            ).first()
            is not None
        )

    async def _load_page(self, client: HTTPXClient, db_conn) -> set[URL]:
        url = self._pending_urls.pop()

        if not await self._robots_file.can_fetch(url, client):
            return set()

        self._timeout = self._robots_file.timeout()

        response = await client.retrying_get(
            url.to_httpx_url(),
            {"accept": "text/html", "accept-language": "de,en"},
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

        if self._exists(str(response.url), db_conn):
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

        return get_links(URL.from_httpx_url(response.url), dom)


class Crawler:
    """The crawler."""

    @staticmethod
    def _get_netloc(url: URL) -> Netloc:
        return url.netloc

    def __getstate__(self) -> dict[str, Any]:
        """Return state used by pickle."""
        return {
            "_crawlers": self._crawlers,
        }

    def __setstate__(self, state: dict[str, Any]):
        """Restore state used by pickle."""
        self.__dict__.update(state)
        self._db_conn = db.ENGINE.connect()
        self._httpx_client = HTTPXClient()
        self._stopping = False

    def __init__(self):
        """Initialize the crawler w/ empty backlog and no connections."""
        self.__setstate__({})
        self._crawlers: dict[Netloc, _SingleNetlocCrawler] = {}

    async def __aenter__(self):
        """Call enter method of database connection and HTTPXClient."""
        self._db_conn.__enter__()
        await self._httpx_client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Close database connection and HTTPXClient."""
        self._db_conn.__exit__(exc_type, exc, tb)
        await self._httpx_client.__aexit__(exc_type, exc, tb)

    def add_url(self, url: URL):
        """Add a URL passed as str to the pending URLs."""
        url = url.normalize()
        if url.netloc not in self._crawlers:
            self._crawlers[url.netloc] = _SingleNetlocCrawler(url.netloc)
        self._crawlers[url.netloc].add_url(url)

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
            for link in get_links(URL.from_string(document.url), dom):
                self.add_url(link)

        return True

    def stop(self):
        """Start stopping all workers."""
        print("Stopping...")
        self._stopping = True

    async def run(self):
        """Work for eternity or until stop is called."""
        tasks: dict[asyncio.Task, Netloc] = {}

        try:
            while not self._stopping:
                for netloc in set(self._crawlers.keys()).difference(
                    tasks.values()
                ):
                    if len(tasks) >= 4:
                        break

                    crawler = self._crawlers[netloc]
                    if not crawler.ready():
                        continue

                    task = asyncio.create_task(
                        crawler._load_page(self._httpx_client, self._db_conn)
                    )
                    tasks[task] = netloc

                done, _ = await asyncio.wait(
                    tasks.keys(), return_when=asyncio.FIRST_COMPLETED
                )

                for task in done:
                    tasks.pop(task)
                    for url in task.result():
                        self.add_url(url)
        finally:
            await asyncio.wait(tasks.keys(), return_when=asyncio.ALL_COMPLETED)
