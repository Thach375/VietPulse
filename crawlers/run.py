"""Crawler chạy thật (Task 9, M2.2): lấy bài báo, bình luận và bài VOZ rồi gửi vào Kafka theo envelope.

Chạy từ máy host (Kafka của docker-compose mở cổng localhost:9094):
    python -m crawlers.run --once                    # quét một lượt rồi thoát
    python -m crawlers.run --interval-minutes 15     # quét lặp lại
Từ một container trong docker-compose thì dùng --bootstrap kafka:9092.

Crawler nhớ khóa Kafka (source:record_id) đã gửi trong SQLite nên chỉ gửi bản ghi mới; một bản ghi chỉ được
đánh dấu là đã gửi khi Kafka xác nhận. Bình luận của mỗi bài được quét ngay khi phát hiện bài, sau đó theo
POLL_SCHEDULE_MIN: 15 phút một lần trong 6 giờ đầu, mỗi giờ một lần đến giờ thứ 48, lần cuối ở giờ thứ 72.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from common.envelope import kafka_key
from crawlers import news, voz
from crawlers.base import POLL_SCHEDULE_MIN, PoliteSession

log = logging.getLogger("crawlers.run")

TOPICS = {
    "news_article": "raw.news.articles",
    "news_comment": "raw.news.comments",
    "voz_post": "raw.forum.posts",
}
# Bài đã đăng quá lần quét cuối khi mới được phát hiện thì không theo dõi bình luận nữa.
TRACK_COMMENTS_MAX_AGE = timedelta(minutes=POLL_SCHEDULE_MIN[-1])


def topic_for(envelope: dict[str, Any]) -> str:
    return TOPICS[envelope["source"]]


def next_poll_time(published: datetime, after: datetime) -> datetime | None:
    """Lần quét bình luận kế tiếp theo lịch, muộn hơn `after`; None nếu đã qua lần quét cuối."""
    for minutes in POLL_SCHEDULE_MIN:
        due = published + timedelta(minutes=minutes)
        if due > after:
            return due
    return None


def _iso(value: datetime) -> str:
    # Mọi mốc lưu ở UTC cùng một định dạng để so sánh chuỗi trong SQLite đúng thứ tự thời gian.
    return value.astimezone(UTC).isoformat()


class Publisher(Protocol):
    def publish(self, envelopes: list[dict[str, Any]]) -> list[str]:
        """Gửi envelope, trả về khóa Kafka của những bản đã được xác nhận."""


class KafkaPublisher:
    def __init__(self, bootstrap: str) -> None:
        from confluent_kafka import Producer  # import muộn để test không cần thư viện Kafka

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 50,
                # docker-compose chỉ mở cổng Kafka trên 127.0.0.1; không thử IPv6 ::1 trước.
                "broker.address.family": "v4",
            }
        )

    def publish(self, envelopes: list[dict[str, Any]]) -> list[str]:
        delivered: list[str] = []

        def on_delivery(err, msg) -> None:
            if err is None:
                delivered.append(msg.key().decode())
            else:
                log.warning("Kafka không nhận %s: %s", msg.key(), err)

        for envelope in envelopes:
            self._producer.produce(
                topic_for(envelope),
                key=kafka_key(envelope).encode(),
                value=json.dumps(envelope, ensure_ascii=False).encode(),
                on_delivery=on_delivery,
            )
            self._producer.poll(0)
        remaining = self._producer.flush(30)
        if remaining:
            log.warning("%d bản ghi chưa được Kafka xác nhận sau 30 giây", remaining)
        return delivered


@dataclass(frozen=True)
class TrackedArticle:
    site: str
    article_id: str
    published: datetime


class CrawlState:
    """Trạng thái bền của crawler trong SQLite: khóa đã gửi và lịch quét bình luận của từng bài."""

    def __init__(self, path: Path | str) -> None:
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(path))
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS sent (key TEXT PRIMARY KEY, sent_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS articles (
                site TEXT NOT NULL,
                article_id TEXT NOT NULL,
                url TEXT NOT NULL,
                published TEXT NOT NULL,
                next_poll TEXT NOT NULL,
                PRIMARY KEY (site, article_id)
            );
            """
        )

    def is_sent(self, key: str) -> bool:
        return self._db.execute("SELECT 1 FROM sent WHERE key = ?", (key,)).fetchone() is not None

    def unsent(self, envelopes: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        """Bỏ bản đã gửi và bản trùng khóa trong cùng lô."""
        fresh, seen = [], set()
        for envelope in envelopes:
            key = kafka_key(envelope)
            if key not in seen and not self.is_sent(key):
                seen.add(key)
                fresh.append(envelope)
        return fresh

    def mark_sent(self, keys: Iterable[str], now: datetime) -> None:
        self._db.executemany(
            "INSERT OR IGNORE INTO sent (key, sent_at) VALUES (?, ?)", [(key, _iso(now)) for key in keys]
        )
        self._db.commit()

    def track_article(self, site: str, article_id: str, url: str, published: datetime, now: datetime) -> None:
        """Theo dõi bình luận của bài mới phát hiện: quét ngay ở lượt này, sau đó theo lịch."""
        if now - published > TRACK_COMMENTS_MAX_AGE:
            return
        self._db.execute(
            "INSERT OR IGNORE INTO articles VALUES (?, ?, ?, ?, ?)",
            (site, article_id, url, _iso(published), _iso(now)),
        )
        self._db.commit()

    def due_articles(self, now: datetime, limit: int) -> list[TrackedArticle]:
        rows = self._db.execute(
            "SELECT site, article_id, published FROM articles"
            " WHERE next_poll <= ? ORDER BY next_poll LIMIT ?",
            (_iso(now), limit),
        ).fetchall()
        return [TrackedArticle(site, aid, datetime.fromisoformat(published)) for site, aid, published in rows]

    def reschedule(self, article: TrackedArticle, now: datetime) -> None:
        due = next_poll_time(article.published, now)
        if due is None:
            self._db.execute(
                "DELETE FROM articles WHERE site = ? AND article_id = ?", (article.site, article.article_id)
            )
        else:
            self._db.execute(
                "UPDATE articles SET next_poll = ? WHERE site = ? AND article_id = ?",
                (_iso(due), article.site, article.article_id),
            )
        self._db.commit()


def publish_new(
    state: CrawlState, publisher: Publisher, envelopes: Iterable[dict[str, Any]], now: datetime
) -> int:
    fresh = state.unsent(envelopes)
    if not fresh:
        return 0
    delivered = publisher.publish(fresh)
    state.mark_sent(delivered, now)
    return len(delivered)


@dataclass
class CycleOptions:
    sites: list[news.SiteConfig]
    max_new_articles: int = 10
    max_comment_polls: int = 60
    voz_posts: int = 100
    comments: bool = True
    voz: bool = True


def run_cycle(
    session: PoliteSession,
    state: CrawlState,
    publisher: Publisher,
    options: CycleOptions,
    now: datetime | None = None,
) -> dict[str, int]:
    """Một lượt quét: bài mới của từng trang, bình luận đến hạn, bài VOZ hot.

    Một nguồn lỗi chỉ được ghi log, không chặn các nguồn khác.
    """
    now = now or datetime.now(UTC)
    stats = {"articles": 0, "comments": 0, "voz_posts": 0}

    for site in options.sites:
        try:
            refs = news.list_articles(session, site)
        except Exception as exc:
            log.warning("[%s] lỗi khi đọc feed: %r", site.name, exc)
            continue
        new_refs = [r for r in refs if not state.is_sent(f"news_article:{site.name}:{r.article_id}")]
        envelopes = []
        for ref in new_refs[: options.max_new_articles]:
            try:
                envelope = news.fetch_article(session, ref)
            except Exception as exc:
                log.warning("[%s] lỗi khi đọc bài %s: %r", site.name, ref.url, exc)
                continue
            if envelope:
                envelopes.append(envelope)
        sent = publish_new(state, publisher, envelopes, now)
        stats["articles"] += sent
        log.info("[%s] %d bài trong feed, gửi %d bài mới", site.name, len(refs), sent)
        if options.comments and site.name in news.COMMENT_FETCHERS:
            for ref in refs:
                if ref.feed_item.published:
                    state.track_article(site.name, ref.article_id, ref.url, ref.feed_item.published, now)

    if options.comments:
        for article in state.due_articles(now, options.max_comment_polls):
            try:
                comments = news.COMMENT_FETCHERS[article.site](session, article.article_id)
            except Exception as exc:
                log.warning("[%s] lỗi khi lấy bình luận bài %s: %r", article.site, article.article_id, exc)
                comments = []
            stats["comments"] += publish_new(state, publisher, comments, now)
            state.reschedule(article, now)
        log.info("[bình luận] gửi %d bình luận mới", stats["comments"])

    if options.voz:
        try:
            posts = voz.fetch_posts(session, limit=options.voz_posts)
            stats["voz_posts"] = publish_new(state, publisher, posts, now)
        except Exception as exc:
            log.warning("[voz] lỗi khi đọc %s: %r", voz.HOT_FEED, exc)
        log.info("[voz] gửi %d bài mới từ %s", stats["voz_posts"], voz.HOT_FEED)

    return stats


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--bootstrap", default="localhost:9094", help="Kafka bootstrap servers")
    parser.add_argument("--state", type=Path, default=Path("data/state/crawler.sqlite"))
    parser.add_argument("--sites", default=",".join(news.SITES), help="các trang báo, cách nhau bởi dấu phẩy")
    parser.add_argument("--once", action="store_true", help="quét một lượt rồi thoát")
    parser.add_argument("--interval-minutes", type=float, default=15)
    parser.add_argument(
        "--max-new-articles", type=int, default=10, help="số bài mới tối đa mỗi trang mỗi lượt"
    )
    parser.add_argument(
        "--max-comment-polls", type=int, default=60, help="số bài được quét bình luận mỗi lượt"
    )
    parser.add_argument("--voz-posts", type=int, default=100)
    parser.add_argument("--no-comments", action="store_true")
    parser.add_argument("--no-voz", action="store_true")
    parser.add_argument(
        "--request-interval", type=float, default=3.0, help="số giây tối thiểu giữa hai request tới cùng host"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    options = CycleOptions(
        sites=[news.SITES[name] for name in args.sites.split(",")],
        max_new_articles=args.max_new_articles,
        max_comment_polls=args.max_comment_polls,
        voz_posts=args.voz_posts,
        comments=not args.no_comments,
        voz=not args.no_voz,
    )
    session = PoliteSession(min_interval=args.request_interval)
    state = CrawlState(args.state)
    publisher = KafkaPublisher(args.bootstrap)
    try:
        while True:
            started = time.monotonic()
            stats = run_cycle(session, state, publisher, options)
            log.info(
                "xong một lượt: %d bài, %d bình luận, %d bài VOZ",
                stats["articles"],
                stats["comments"],
                stats["voz_posts"],
            )
            if args.once:
                break
            time.sleep(max(0.0, args.interval_minutes * 60 - (time.monotonic() - started)))
    except KeyboardInterrupt:
        log.info("dừng theo yêu cầu")


if __name__ == "__main__":
    main()
