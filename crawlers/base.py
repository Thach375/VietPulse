"""Phần dùng chung của crawler: HTTP lịch sự, đọc RSS, làm sạch HTML, băm định danh người dùng."""

from __future__ import annotations

import hashlib
import time
import urllib.robotparser
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, tzinfo
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

import requests
from bs4 import BeautifulSoup

# Lịch quét lại bình luận của mỗi bài, tính bằng phút từ giờ đăng (M1.5): 15 phút một lần trong 6 giờ đầu,
# mỗi giờ một lần đến giờ thứ 48, lần cuối ở giờ thứ 72.
POLL_SCHEDULE_MIN = tuple(range(15, 361, 15)) + tuple(range(420, 2881, 60)) + (4320,)

USER_AGENT = (
    "VietPulse-research-bot/0.1 (+https://github.com/Thach375/VietPulse; HUST IT4043E student project)"
)


class PoliteSession:
    """Session HTTP giãn cách request theo từng host và kiểm robots.txt trước khi gọi.

    robotparser của Python chỉ so khớp tiền tố, không hiểu ký tự đại diện như `/t/*/post-`
    trong robots.txt của VOZ, nên crawler chỉ tự dựng những dạng URL đã đối chiếu tay.
    """

    def __init__(
        self, min_interval: float = 1.0, timeout: float = 20.0, retry_backoff: tuple[float, ...] = (5.0, 15.0)
    ) -> None:
        self._http = requests.Session()
        self._http.headers["User-Agent"] = USER_AGENT
        self._min_interval = min_interval
        self._timeout = timeout
        self._retry_backoff = retry_backoff
        self._last_call: dict[str, float] = {}
        self._robots: dict[str, urllib.robotparser.RobotFileParser] = {}

    def get(self, url: str, **kwargs) -> requests.Response:
        if not self._allowed(url):
            raise PermissionError(f"robots.txt không cho phép: {url}")
        # Tuổi Trẻ, Thanh Niên, Kenh14 thỉnh thoảng ngắt kết nối giữa chừng: chờ rồi thử lại vài lần.
        for backoff in (*self._retry_backoff, None):
            self._wait(urlsplit(url).netloc)
            try:
                resp = self._http.get(url, timeout=self._timeout, **kwargs)
                break
            except requests.ConnectionError:
                if backoff is None:
                    raise
                time.sleep(backoff)
        resp.raise_for_status()
        # Thiếu charset trong header thì requests đoán ISO-8859-1 và làm hỏng tiếng Việt.
        if "charset" not in resp.headers.get("Content-Type", "").lower():
            resp.encoding = "utf-8"
        return resp

    def _wait(self, host: str) -> None:
        elapsed = time.monotonic() - self._last_call.get(host, 0.0)
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_call[host] = time.monotonic()

    def _allowed(self, url: str) -> bool:
        parts = urlsplit(url)
        root = f"{parts.scheme}://{parts.netloc}"
        parser = self._robots.get(root)
        if parser is None:
            parser = urllib.robotparser.RobotFileParser()
            self._wait(parts.netloc)
            resp = self._http.get(root + "/robots.txt", timeout=self._timeout)
            if resp.status_code in (401, 403):
                parser.disallow_all = True
            elif resp.status_code >= 400:
                parser.allow_all = True
            elif resp.text.lstrip().startswith("<"):
                # Trang chống bot trả HTML thay cho robots.txt (ví dụ Lao Động): coi như không được phép.
                parser.disallow_all = True
            else:
                parser.parse(resp.text.splitlines())
            self._robots[root] = parser
        return parser.can_fetch(USER_AGENT, url)


@dataclass(frozen=True)
class FeedItem:
    title: str
    link: str
    summary: str
    published: datetime | None


def parse_rss(xml: str | bytes, default_tz: tzinfo | None = None) -> list[FeedItem]:
    """Đọc RSS. Nên truyền bytes để XML tự khai báo encoding.

    `default_tz` gán cho pubDate không kèm múi giờ, như của Tuổi Trẻ (`10/18/2026 8:21:00 PM`).
    """
    root = ET.fromstring(_strip_bom(xml))
    items = []
    for node in root.iter("item"):
        items.append(
            FeedItem(
                title=(node.findtext("title") or "").strip(),
                link=(node.findtext("link") or "").strip(),
                summary=html_to_text(node.findtext("description") or ""),
                published=_parse_pub_date(node.findtext("pubDate"), default_tz),
            )
        )
    return items


_SITEMAP_NS = {
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
    "news": "http://www.google.com/schemas/sitemap-news/0.9",
}


def parse_news_sitemap(xml: str | bytes) -> list[FeedItem]:
    """Đọc Google News sitemap, dùng cho trang không có RSS như ZNews."""
    root = ET.fromstring(_strip_bom(xml))
    items = []
    for node in root.findall("sm:url", _SITEMAP_NS):
        published_text = (node.findtext("news:news/news:publication_date", "", _SITEMAP_NS) or "").strip()
        try:
            published = datetime.fromisoformat(published_text) if published_text else None
        except ValueError:
            published = None
        items.append(
            FeedItem(
                title=(node.findtext("news:news/news:title", "", _SITEMAP_NS) or "").strip(),
                link=(node.findtext("sm:loc", "", _SITEMAP_NS) or "").strip(),
                summary="",
                published=published if published and published.tzinfo else None,
            )
        )
    return items


def _strip_bom(xml: str | bytes) -> str | bytes:
    if isinstance(xml, bytes):
        return xml.lstrip(b"\xef\xbb\xbf").strip()
    return xml.lstrip("﻿").strip()


def _parse_pub_date(value: str | None, default_tz: tzinfo | None) -> datetime | None:
    if not value:
        return None
    value = value.replace(" ", " ").strip()
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        try:
            parsed = datetime.strptime(value, "%m/%d/%Y %I:%M:%S %p")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=default_tz) if default_tz else None
    return parsed


def html_to_text(html: str) -> str:
    return BeautifulSoup(html, "html.parser").get_text(" ", strip=True)


def hash_user(site: str, raw_id: str | None) -> str | None:
    """Băm định danh người dùng để không lưu tên hay email dạng gốc."""
    if not raw_id:
        return None
    return hashlib.sha256(f"{site}:{raw_id}".encode()).hexdigest()[:16]
