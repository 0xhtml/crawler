"""The crawler that can crawl the internet."""

import asyncio
import pickle
import re

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

    return True


class Crawler:
    """The crawler."""

    _NEWLINE_REGEX = re.compile(rb"\n+")

    def __init__(self):
        """Connect to database and init HTTPXClient."""
        self._db_conn = db.ENGINE.connect()
        self._httpx_client = HTTPXClient()
        self._robots_file_table = RobotsFileTable(self._httpx_client)

        def get_netloc(url: URL) -> bytes:
            return url.netloc

        self._pending_urls = BucketSet(get_netloc)

        self._stopping = False
        self._condition = asyncio.Condition()
        self._active_urls: set[URL] = set()

    async def __aenter__(self):
        """Call enter method of database connection and HTTPXClient."""
        self._db_conn.__enter__()
        await self._httpx_client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Close database connection and HTTPXClient."""
        self._db_conn.__exit__(exc_type, exc, tb)
        await self._httpx_client.__aexit__(exc_type, exc, tb)

    def load_urls_pkl(self, filename: str) -> bool:
        """Try loading URLs from the pkl file returning True on success."""
        try:
            with open(filename, "rb") as file:
                self._pending_urls._dict = pickle.load(file)
            return True
        except FileNotFoundError:
            return False

    def load_urls_db(self):
        """Load the URLs out of the database (slow)."""
        for document in tqdm(
            self._db_conn.execute(sqlalchemy.select(db.DOCUMENTS_TABLE)),
            total=self._db_conn.execute(
                sqlalchemy.func.count(db.DOCUMENTS_TABLE.c.url)
            ).scalar(),
            ncols=100,
        ):
            dom = etree.fromstring(document.content, parser=html.html_parser)
            self._pending_urls.update(get_links(URL(document.url), dom))

    def dump_urls_pkl(self, filename: str):
        """Dump URLs to the pkl file."""
        with open(filename, "wb") as file:
            pickle.dump(self._pending_urls._dict, file)

    async def stop(self):
        """Start stopping all workers."""
        print("Stopping...")
        self._stopping = True
        async with self._condition:
            self._condition.notify_all()

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
        if head_response is None or not _is_valid_response(head_response):
            return set()

        url = normalize_url(head_response.url, head_response.history)
        if self._exists(url):
            return set()

        get_response = await self._httpx_client.retrying_get(url)
        if get_response is None or not _is_valid_response(get_response):
            return set()

        get_response.history[0:0] = head_response.history
        url = normalize_url(get_response.url, get_response.history)
        if self._exists(url):
            return set()

        dom = etree.fromstring(get_response.content, parser=html.html_parser)
        if dom is None:
            print(f"SKIP {str(url)[:80]} dom is None")
            return set()

        HTML_CLEANER(dom)

        if get_lang(dom) not in {"de", "en"}:
            print(f"SKIP {str(url)[:80]} lang isn't de or en")
            return set()

        self._db_conn.execute(
            sqlalchemy.insert(db.DOCUMENTS_TABLE).values(
                url=str(url),
                content=self._NEWLINE_REGEX.sub(b"\n", html.tostring(dom)),
            )
        )

        return get_links(url, dom)

    async def worker(self):
        """Work for eternity or until stop is called."""
        while not self._stopping:
            while not (
                urls := self._pending_urls.key_difference(self._active_urls)
            ):
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
