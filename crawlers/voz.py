"""Crawler diễn đàn VOZ (XenForo 2).

Mặc định lấy thread từ trang `/feeds/hot` (các thread đang nóng của mọi box). Chỉ đọc trang danh sách
`/feeds/hot`, `/f/<slug>.<id>/` và trang thread `/t/<slug>.<id>/page-<n>`, là những dạng URL robots.txt
của VOZ cho phép. Không gọi `/t/*/post-`, `/p/`, `/search/`.
VOZ khai báo `Content-Signal: ai-train=no`: dữ liệu VOZ chỉ dùng để suy luận, không dùng để fine-tune model.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from bs4 import BeautifulSoup

from common.envelope import make_envelope
from crawlers.base import PoliteSession, hash_user

BASE_URL = "https://voz.vn"
HOT_FEED = "/feeds/hot"
FORUMS = {33: "/f/diem-bao.33/", 17: "/f/chuyen-tro-linh-tinhtm.17/", 92: "/f/kinh-te-luat.92/"}
_THREAD_HREF = re.compile(r"^/t/[^/]+\.(\d+)/$")


@dataclass(frozen=True)
class ThreadRef:
    thread_id: str
    url: str
    title: str
    sticky: bool


def parse_forum_page(html: str) -> list[ThreadRef]:
    soup = BeautifulSoup(html, "html.parser")
    threads = []
    for item in soup.select("div.structItem--thread"):
        link = next(
            (a for a in item.select("div.structItem-title a[href]") if _THREAD_HREF.match(a["href"])), None
        )
        if link is None:
            continue
        sticky = item.find_parent(class_="structItemContainer-group--sticky") is not None
        thread_id = _THREAD_HREF.match(link["href"]).group(1)
        threads.append(ThreadRef(thread_id, BASE_URL + link["href"], link.get_text(strip=True), sticky))
    return threads


def parse_thread_page(html: str, thread: ThreadRef, forum_id: int | None) -> list[dict[str, Any]]:
    """Đổi các bài trong một trang thread thành envelope; phần trích dẫn bài khác bị bỏ khỏi nội dung."""
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for article in soup.select("article.message--post"):
        post_id = (article.get("data-content") or "").removeprefix("post-")
        stamp = article.select_one("header.message-attribution time.u-dt") or article.select_one("time.u-dt")
        wrapper = article.select_one("div.bbWrapper")
        if not post_id or stamp is None or wrapper is None:
            continue
        for quote in wrapper.select("blockquote"):
            quote.decompose()
        out.append(
            make_envelope(
                source="voz_post",
                record_id=f"voz:{post_id}",
                event_time=datetime.fromtimestamp(int(stamp["data-timestamp"]), UTC),
                payload={
                    "site": "voz",
                    "post_id": post_id,
                    "forum_id": forum_id,
                    "thread_id": thread.thread_id,
                    "thread_title": thread.title,
                    "url": thread.url,
                    "content": wrapper.get_text(" ", strip=True),
                    "author_hash": hash_user("voz", article.get("data-author")),
                },
            )
        )
    return out


def fetch_posts(
    session: PoliteSession, limit: int, listing: str = HOT_FEED, forum_id: int | None = None
) -> list[dict[str, Any]]:
    """Lấy bài từ trang 1 của các thread trong trang danh sách (bỏ thread ghim) cho đến khi đủ `limit`.

    Thread lấy từ `/feeds/hot` thuộc nhiều box khác nhau nên `forum_id` để trống.
    """
    threads = parse_forum_page(session.get(BASE_URL + listing).text)
    posts: list[dict[str, Any]] = []
    for thread in threads:
        if thread.sticky:
            continue
        posts.extend(parse_thread_page(session.get(thread.url).text, thread, forum_id))
        if len(posts) >= limit:
            break
    return posts[:limit]
