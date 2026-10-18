"""Crawl thử cho M1.5: lấy mẫu mỗi nguồn và đo bình luận đến muộn bao lâu so với giờ đăng bài.

Chạy: python -m crawlers.sample --out data/samples
Kết quả: <out>/<source>.jsonl (mỗi dòng một envelope) và <out>/lateness.json.
"""

from __future__ import annotations

import argparse
import json
import statistics
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from crawlers import news, voz
from crawlers.base import PoliteSession

DELAY_THRESHOLDS_H = (1, 2, 6, 24, 48)


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _percentile(sorted_values: list[float], q: float) -> float:
    idx = min(len(sorted_values) - 1, max(0, round(q * (len(sorted_values) - 1))))
    return sorted_values[idx]


def summarize_delays(delays_min: list[float]) -> dict[str, Any]:
    if not delays_min:
        return {"count": 0}
    values = sorted(delays_min)
    summary: dict[str, Any] = {
        "count": len(values),
        "negative": sum(1 for v in values if v < 0),
        "p50_min": round(statistics.median(values), 1),
    }
    for q in (0.75, 0.9, 0.95, 0.99):
        summary[f"p{int(q * 100)}_min"] = round(_percentile(values, q), 1)
    summary["max_min"] = round(values[-1], 1)
    for hours in DELAY_THRESHOLDS_H:
        summary[f"share_over_{hours}h"] = round(sum(1 for v in values if v > hours * 60) / len(values), 3)
    return summary


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("data/samples"))
    parser.add_argument("--per-source", type=int, default=100, help="số bài báo và số bài VOZ cần lấy")
    parser.add_argument(
        "--comment-articles", type=int, default=120, help="số bài báo mỗi trang dùng để lấy bình luận"
    )
    parser.add_argument("--sites", default=",".join(news.SITES), help="các trang báo, cách nhau bởi dấu phẩy")
    parser.add_argument("--interval", type=float, default=1.0, help="số giây tối thiểu giữa hai request")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    session = PoliteSession(min_interval=args.interval)
    sites = [news.SITES[name] for name in args.sites.split(",")]
    now = datetime.now(UTC)

    articles: list[dict[str, Any]] = []
    comments: list[dict[str, Any]] = []
    lateness: dict[str, Any] = {"crawled_at": now.isoformat(), "sites": {}}
    per_site = args.per_source // len(news.SITES)

    for site in sites:
        refs = news.list_articles(session, site)
        print(f"[{site.name}] {len(refs)} bài trong RSS")
        site_articles = []
        for ref in refs[:per_site]:
            envelope = news.fetch_article(session, ref)
            if envelope:
                site_articles.append(envelope)
        articles.extend(site_articles)

        # Ưu tiên bài cũ nhất trong RSS để bình luận kịp "chín" trước khi đo độ trễ.
        dated = sorted((r for r in refs if r.feed_item.published), key=lambda r: r.feed_item.published)
        delays_all, delays_old, moderation = [], [], []
        site_comments = []
        for ref in dated[: args.comment_articles]:
            published = ref.feed_item.published
            age_h = (now - published).total_seconds() / 3600
            for c in news.COMMENT_FETCHERS[site.name](session, ref.article_id):
                site_comments.append(c)
                delay = (_ts(c["event_time"]) - published).total_seconds() / 60
                delays_all.append(delay)
                if age_h >= 24:
                    delays_old.append(delay)
                visible = c["payload"].get("published_time")
                if visible:
                    moderation.append((_ts(visible) - _ts(c["event_time"])).total_seconds() / 60)
        comments.extend(site_comments)
        lateness["sites"][site.name] = {
            "articles_with_body": sum(1 for a in site_articles if a["payload"]["body"]),
            "articles_sampled": len(site_articles),
            "comment_articles": min(len(dated), args.comment_articles),
            "comments": len(site_comments),
            "delay_vs_article_all": summarize_delays(delays_all),
            "delay_vs_article_age_ge_24h": summarize_delays(delays_old),
            "moderation_delay": summarize_delays(moderation),
        }
        print(f"[{site.name}] {len(site_articles)} bài, {len(site_comments)} bình luận")

    posts = voz.fetch_posts(session, forum_id=33, limit=args.per_source)
    print(f"[voz] {len(posts)} bài từ box Điểm báo")

    write_jsonl(args.out / "news_article.jsonl", articles)
    write_jsonl(args.out / "news_comment.jsonl", comments)
    write_jsonl(args.out / "voz_post.jsonl", posts)
    lateness["voz_posts"] = len(posts)
    (args.out / "lateness.json").write_text(json.dumps(lateness, ensure_ascii=False, indent=2), "utf-8")
    print(json.dumps(lateness, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
