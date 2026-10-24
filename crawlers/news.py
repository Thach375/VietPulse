"""Crawler báo điện tử: bài viết qua RSS hoặc Google News sitemap, bình luận qua API công khai.

Danh sách báo lấy theo https://miccreative.vn/cac-trang-bao-chinh-thong/, chỉ giữ những trang có RSS
hoặc sitemap tin tức đọc được (kết quả dò ở docs/m1.5-nguon-du-lieu.md). Bình luận hiện lấy được từ
VnExpress và Tuổi Trẻ, vì hai trang này có API bình luận trả JSON.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

from common.envelope import make_envelope
from crawlers.base import (
    FeedItem,
    PoliteSession,
    hash_user,
    html_to_text,
    parse_news_sitemap,
    parse_rss,
)

VN_TZ = timezone(timedelta(hours=7))
_CATEGORIES = ("thoi-su", "the-gioi", "kinh-doanh", "phap-luat", "giao-duc", "suc-khoe")


@dataclass(frozen=True)
class SiteConfig:
    name: str
    feeds: tuple[str, ...]
    feed_format: str = "rss"  # "rss" hoặc "news_sitemap"
    body_selector: str | None = None


SITES = {
    site.name: site
    for site in (
        SiteConfig(
            "vnexpress",
            tuple(f"https://vnexpress.net/rss/{c}.rss" for c in _CATEGORIES),
            body_selector="article.fck_detail p.Normal",
        ),
        SiteConfig(
            "tuoitre",
            tuple(f"https://tuoitre.vn/rss/{c}.rss" for c in _CATEGORIES),
            body_selector="div.detail-content p",
        ),
        SiteConfig("dantri", ("https://dantri.com.vn/rss/home.rss",)),
        SiteConfig("vietnamnet", ("https://vietnamnet.vn/home.rss",), body_selector="div.maincontent p"),
        SiteConfig("thanhnien", ("https://thanhnien.vn/rss/home.rss",)),
        SiteConfig("tienphong", ("https://tienphong.vn/rss/home.rss",)),
        SiteConfig("nhandan", ("https://nhandan.vn/rss/home.rss",), body_selector="div.article__body p"),
        SiteConfig("cand", ("https://cand.vn/rss/home.rss",)),
        SiteConfig("sggp", ("https://www.sggp.org.vn/rss/home.rss",), body_selector="div.article__body p"),
        SiteConfig("kenh14", ("https://kenh14.vn/rss/home.rss",), body_selector="div.detail-content p"),
        SiteConfig(
            "doisongphapluat",
            ("https://doisongphapluat.com.vn/rss/tin-tuc.rss",),
            body_selector="div.edittor-content p",
        ),
        SiteConfig("znews", ("https://znews.vn/sitemap/sitemap-news.xml",), feed_format="news_sitemap"),
    )
}

# Phần lớn báo kết thúc URL bài bằng id số: -5136488.html, -post1888243.tpo, -a736336.html,
# -100261024194757437.htm.
_ARTICLE_ID = re.compile(r"-(?:post|a)?(\d{5,})\.[a-z]+$")
_ARTICLE_BODY = re.compile(r'"articleBody"\s*:\s*"((?:[^"\\]|\\.)*)"')
_PUBLISHED_PATTERNS = (
    re.compile(r'"datePublished"\s*:\s*"([^"]+)"'),
    re.compile(r'property="article:published_time"\s+content="([^"]+)"'),
    re.compile(r'itemprop="datePublished"\s+content="([^"]+)"'),
)


def article_id(url: str) -> str:
    """Lấy id bài từ URL; URL không có id số thì dùng bản băm của đường dẫn."""
    path = urlsplit(url).path
    match = _ARTICLE_ID.search(path)
    if match:
        return match.group(1)
    return hashlib.sha1(path.encode()).hexdigest()[:16]


@dataclass(frozen=True)
class ArticleRef:
    site: str
    article_id: str
    url: str
    category: str
    feed_item: FeedItem


def list_articles(session: PoliteSession, site: SiteConfig) -> list[ArticleRef]:
    """Đọc mọi feed của một trang, bỏ trùng theo article_id."""
    refs: dict[str, ArticleRef] = {}
    for feed in site.feeds:
        content = session.get(feed).content
        if site.feed_format == "news_sitemap":
            items, category = parse_news_sitemap(content), "news"
        else:
            items = parse_rss(content, default_tz=VN_TZ)
            category = feed.rsplit("/", 1)[-1].removesuffix(".rss")
        for item in items:
            if not item.link:
                continue
            aid = article_id(item.link)
            if aid not in refs:
                refs[aid] = ArticleRef(site.name, aid, item.link, category, item)
    return list(refs.values())


def parse_article_page(html: str, body_selector: str | None = None) -> tuple[str, datetime | None]:
    """Lấy thân bài và thời điểm đăng (có múi giờ) từ HTML trang bài báo.

    Thân bài lấy theo thứ tự: selector riêng của trang, `articleBody` trong JSON-LD, các thẻ `article p`.
    """
    soup = BeautifulSoup(html, "html.parser")
    body = ""
    if body_selector:
        body = "\n".join(p.get_text(" ", strip=True) for p in soup.select(body_selector))
    if not body:
        match = _ARTICLE_BODY.search(html)
        if match:
            try:
                body = html_to_text(json.loads(f'"{match.group(1)}"'))
            except ValueError:
                body = ""
    if not body:
        body = "\n".join(p.get_text(" ", strip=True) for p in soup.select("article p"))
    published = None
    for pattern in _PUBLISHED_PATTERNS:
        match = pattern.search(html)
        if match:
            try:
                published = datetime.fromisoformat(match.group(1))
            except ValueError:
                continue
            if published.tzinfo is None:
                published = published.replace(tzinfo=VN_TZ)
            break
    return body.strip(), published


def article_envelope(ref: ArticleRef, body: str, published: datetime) -> dict[str, Any]:
    return make_envelope(
        source="news_article",
        record_id=f"{ref.site}:{ref.article_id}",
        event_time=published,
        payload={
            "site": ref.site,
            "article_id": ref.article_id,
            "url": ref.url,
            "category": ref.category,
            "title": ref.feed_item.title,
            "summary": ref.feed_item.summary,
            "body": body,
        },
    )


def fetch_article(session: PoliteSession, ref: ArticleRef) -> dict[str, Any] | None:
    body, published = parse_article_page(session.get(ref.url).text, SITES[ref.site].body_selector)
    published = published or ref.feed_item.published
    if published is None:
        return None
    return article_envelope(ref, body, published)


# --- Bình luận VnExpress ------------------------------------------------------------------

VNE_COMMENT_API = "https://usi-saas.vnexpress.net/index/get"


def parse_vnexpress_comments(data: dict[str, Any], article_id: str) -> list[dict[str, Any]]:
    """Đổi JSON của API usi-saas thành envelope. API này chỉ trả bình luận gốc, không trả trả lời."""
    out = []
    for item in (data.get("data") or {}).get("items") or []:
        out.append(
            make_envelope(
                source="news_comment",
                record_id=f"vnexpress:{item['comment_id']}",
                event_time=datetime.fromtimestamp(int(item["creation_time"]), UTC),
                payload={
                    "site": "vnexpress",
                    "comment_id": str(item["comment_id"]),
                    "article_id": article_id,
                    "parent_id": str(item.get("parent_id") or item["comment_id"]),
                    "content": html_to_text(item.get("content") or ""),
                    "author_hash": hash_user("vnexpress", str(item.get("userid") or "")),
                    "likes": int(item.get("userlike") or 0),
                },
            )
        )
    return out


def fetch_vnexpress_comments(
    session: PoliteSession, article_id: str, page_size: int = 50, max_pages: int = 20
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for page in range(max_pages):
        params = {
            "offset": page * page_size,
            "limit": page_size,
            "frommobile": 0,
            "sort": "like",
            "is_onload": 1,
            "objectid": article_id,
            "objecttype": 1,
            "siteid": 1000000,
        }
        batch = parse_vnexpress_comments(session.get(VNE_COMMENT_API, params=params).json(), article_id)
        out.extend(batch)
        if len(batch) < page_size:
            break
    return out


# --- Bình luận Tuổi Trẻ -------------------------------------------------------------------

TT_COMMENT_API = "https://id.tuoitre.vn/api/getlist-comment.api"


def _tt_time(value: str | None) -> datetime | None:
    """API Tuổi Trẻ trả giờ Việt Nam không kèm múi giờ, phần lẻ giây có thể dài 7 chữ số."""
    if not value:
        return None
    head, _, frac = value.partition(".")
    parsed = datetime.fromisoformat(head + (f".{frac[:6]}" if frac else ""))
    return parsed.replace(tzinfo=VN_TZ)


def parse_tuoitre_comments(items: list[dict[str, Any]], article_id: str) -> list[dict[str, Any]]:
    """Đổi danh sách bình luận (kể cả trả lời lồng trong child_comments) thành envelope."""
    out = []
    stack = list(items)
    while stack:
        item = stack.pop(0)
        stack.extend(item.get("child_comments") or [])
        created = _tt_time(item.get("created_date"))
        if created is None:
            continue
        published = _tt_time(item.get("published_date"))
        out.append(
            make_envelope(
                source="news_comment",
                record_id=f"tuoitre:{item['id']}",
                event_time=created,
                payload={
                    "site": "tuoitre",
                    "comment_id": str(item["id"]),
                    "article_id": article_id,
                    "parent_id": None if str(item.get("parent_id") or "0") == "0" else str(item["parent_id"]),
                    "content": html_to_text(item.get("content") or ""),
                    "author_hash": hash_user("tuoitre", item.get("user_id") or item.get("sender_fullname")),
                    "likes": int(item.get("likes") or 0),
                    "published_time": published.astimezone(UTC).isoformat().replace("+00:00", "Z")
                    if published
                    else None,
                },
            )
        )
    return out


def fetch_tuoitre_comments(
    session: PoliteSession, article_id: str, page_size: int = 50, max_pages: int = 20
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        params = {"pageindex": page, "pagesize": page_size, "objId": article_id, "objType": 1, "sort": 2}
        items = json.loads(session.get(TT_COMMENT_API, params=params).json().get("Data") or "[]")
        out.extend(parse_tuoitre_comments(items, article_id))
        if len(items) < page_size:
            break
    return out


COMMENT_FETCHERS = {"vnexpress": fetch_vnexpress_comments, "tuoitre": fetch_tuoitre_comments}
