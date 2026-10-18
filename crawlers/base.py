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

USER_AGENT = (
    "VietPulse-research-bot/0.1 (+https://github.com/Thach375/VietPulse; HUST IT4043E student project)"
)


class PoliteSession:
    """Session HTTP giãn cách request theo từng host và kiểm robots.txt trước khi gọi.

    robotparser của Python chỉ so khớp tiền tố, không hiểu ký tự đại diện như `/t/*/post-`
    trong robots.txt của VOZ, nên crawler chỉ tự dựng những dạng URL đã đối chiếu tay.
    """

    def __init__(self, min_interval: float = 1.0, timeout: float = 20.0) -> None:
        self._http = requests.Session()
        self._http.headers["User-Agent"] = USER_AGENT
        self._min_interval = min_interval
        self._timeout = timeout
        self._last_call: dict[str, float] = {}
        self._robots: dict[str, urllib.robotparser.RobotFileParser] = {}

    def get(self, url: str, **kwargs) -> requests.Response:
        if not self._allowed(url):
            raise PermissionError(f"robots.txt không cho phép: {url}")
        self._wait(urlsplit(url).netloc)
        resp = self._http.get(url, timeout=self._timeout, **kwargs)
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
    if isinstance(xml, bytes):
        xml = xml.lstrip(b"\xef\xbb\xbf").strip()
    else:
        xml = xml.lstrip("﻿").strip()
    root = ET.fromstring(xml)
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
