"""Bulk indexing must preserve IDs, dates and per-item failures without leaking credentials."""

import json
from datetime import UTC, datetime

import pytest

from jobs import es_http


def test_ndjson_bulk_actions_have_stable_ids_and_utc_dates():
    body = es_http.bulk_payload(
        [
            {
                "document_id": "news_comment:42",
                "index_day": "2026.10.25",
                "event_time": datetime(2026, 10, 25, 4, 0, tzinfo=UTC),
                "content": "rất tốt",
            }
        ],
        "vp-dlq",
        "index_day",
    )
    assert body.endswith(b"\n")
    action, document = [json.loads(line) for line in body.splitlines()]
    assert action == {"index": {"_index": "vp-dlq-2026.10.25", "_id": "news_comment:42"}}
    assert document["event_time"] == "2026-10-25T04:00:00Z"
    assert document["content"] == "rất tốt"
    assert "index_day" not in document


def test_bulk_item_failure_stops_streaming_batch(monkeypatch):
    calls = []

    def request(url, path, body, method, content_type):
        calls.append((path, body, method, content_type))
        return {"errors": True, "items": [{"index": {"status": 409}}]}

    monkeypatch.setattr(es_http, "request_json", request)

    class Row:
        def asDict(self, recursive=False):
            return {"document_id": "id", "value": 1}

    with pytest.raises(RuntimeError, match="409"):
        es_http.bulk_index_partition([Row()], "https://es:9200", "vp-docs")
    assert calls[0][0] == "/_bulk"
    assert calls[0][3] == "application/x-ndjson"


def test_password_file_cannot_be_sent_over_http(monkeypatch, tmp_path):
    password = tmp_path / "password"
    password.write_text("private-value")
    monkeypatch.setenv("VP_ES_PASSWORD_FILE", str(password))
    with pytest.raises(ValueError, match="HTTPS"):
        es_http.request_json("http://es:9200", "/_cluster/health")
