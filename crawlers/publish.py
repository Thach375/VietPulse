"""Fetch real records and publish acknowledged envelopes, with durable deduplication.

The producer is idempotent within a session. SQLite is updated only after the
Kafka acknowledgement; a crash between those two commits can replay a record.
Spark deduplication and stable serving IDs handle that at-least-once boundary.
https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html
"""

import argparse
import fcntl
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from common.envelope import kafka_key, validate_envelope
from crawlers import news, voz
from crawlers.base import PoliteSession

TOPICS = {
    "news_article": "raw.news.articles",
    "news_comment": "raw.news.comments",
    "voz_post": "raw.forum.posts",
}


def fetch_records(source, sites, limit, interval):
    session = PoliteSession(min_interval=interval)
    if source == "voz":
        yield from voz.fetch_posts(session, limit)
        return
    for site_name in sites:
        site = news.SITES[site_name]
        refs = news.list_articles(session, site)
        if source == "news-articles":
            for ref in refs[:limit]:
                record = news.fetch_article(session, ref)
                if record:
                    yield record
        elif site_name in news.COMMENT_FETCHERS:
            oldest = datetime.now(UTC) - timedelta(hours=72)
            refs = sorted(
                (ref for ref in refs if ref.feed_item.published and ref.feed_item.published >= oldest),
                key=lambda ref: ref.feed_item.published,
            )
            for ref in refs[:limit]:
                yield from news.COMMENT_FETCHERS[site_name](session, ref.article_id)


def publish_records(records, producer, state_file: Path, limit: int):
    state_file.parent.mkdir(parents=True, exist_ok=True)
    receipts = []
    with state_file.with_suffix(".lock").open("a") as lock, sqlite3.connect(state_file) as database:
        # Also excludes a manual run racing the CronJob on the same source PVC.
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        database.execute("CREATE TABLE IF NOT EXISTS sent (record_key TEXT PRIMARY KEY)")
        for record in records:
            validate_envelope(record)
            key = kafka_key(record)
            if database.execute("SELECT 1 FROM sent WHERE record_key=?", (key,)).fetchone():
                continue
            topic = TOPICS[record["source"]]
            delivered = []

            def acknowledgement(error, message, results=delivered):
                results.append((error, message))

            producer.produce(
                topic, key=key, value=json.dumps(record, ensure_ascii=False), on_delivery=acknowledgement
            )
            remaining = producer.flush(45)
            if remaining or len(delivered) != 1 or delivered[0][0] is not None:
                raise RuntimeError("Kafka did not acknowledge the crawler envelope; state was not advanced")
            message = delivered[0][1]
            database.execute("INSERT INTO sent(record_key) VALUES (?)", (key,))
            database.commit()
            receipts.append(
                {
                    "topic": topic,
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "key": key,
                    "envelope": record,
                }
            )
            if len(receipts) >= limit:
                break
    return receipts


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("news-articles", "news-comments", "voz"), required=True)
    parser.add_argument("--bootstrap-servers", default="vietpulse-kafka-bootstrap.messaging.svc:9092")
    parser.add_argument("--sites", default="vnexpress,tuoitre")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--candidate-limit", type=int, default=100)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--state-dir", type=Path, default=Path("/state"))
    parser.add_argument("--require-record", action="store_true")
    parser.add_argument("--print-receipt", action="store_true")
    args = parser.parse_args(argv)
    sites = args.sites.split(",")
    if (
        args.limit < 1
        or args.candidate_limit < 1
        or args.interval < 1
        or any(site not in news.SITES for site in sites)
    ):
        parser.error("positive limits, known sites and an interval of at least one second are required")
    from confluent_kafka import Producer

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": 45000,
        }
    )
    receipts = publish_records(
        fetch_records(args.source, sites, args.candidate_limit, args.interval),
        producer,
        args.state_dir / f"{args.source}.sqlite",
        args.limit,
    )
    if args.require_record and not receipts:
        raise RuntimeError("Crawler produced no new real envelope; ingestion acceptance did not pass")
    print(json.dumps({"source": args.source, "acknowledged": len(receipts)}))
    if args.print_receipt and receipts:
        print(json.dumps({"crawler_receipt": receipts[0]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
