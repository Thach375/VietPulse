from datetime import UTC, datetime

import pytest

from common.envelope import make_envelope
from crawlers.publish import publish_records


class Message:
    def partition(self):
        return 1

    def offset(self):
        return 42


class Producer:
    def __init__(self, error=None):
        self.error = error
        self.sent = []

    def produce(self, topic, key, value, on_delivery):
        self.sent.append((topic, key, value))
        self.callback = on_delivery

    def flush(self, timeout):
        self.callback(self.error, Message())
        return 0


def test_restart_skips_only_acknowledged_envelopes(tmp_path):
    record = make_envelope("news_article", "42", datetime.now(UTC), {"title": "tin thật"})
    first = Producer(error="unavailable")
    with pytest.raises(RuntimeError, match="not acknowledge"):
        publish_records([record], first, tmp_path / "state.sqlite", 1)
    second = Producer()
    receipts = publish_records([record], second, tmp_path / "state.sqlite", 1)
    assert receipts[0]["topic"] == "raw.news.articles"
    assert receipts[0]["key"] == "news_article:42"
    assert receipts[0]["offset"] == 42
    third = Producer()
    assert publish_records([record], third, tmp_path / "state.sqlite", 1) == []
    assert third.sent == []


def test_invalid_envelope_never_reaches_kafka(tmp_path):
    producer = Producer()
    with pytest.raises(ValueError):
        publish_records([{"source": "wrong"}], producer, tmp_path / "state.sqlite", 1)
    assert producer.sent == []
