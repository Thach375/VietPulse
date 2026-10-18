"""Envelope chung cho mọi bản ghi gửi vào Kafka.

Đây là bản code của hợp đồng trong `contracts/envelope.schema.json` (mục 3 của kế hoạch).
Đổi file này phải theo quy trình đổi hợp đồng trong CONTRIBUTING.md.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

SCHEMA_VERSION = 1
SOURCES = frozenset({"news_article", "news_comment", "voz_post", "youtube_comment"})
REQUIRED_FIELDS = ("source", "record_id", "event_time", "ingest_time", "schema_version", "payload")


class EnvelopeError(ValueError):
    """Bản ghi không đúng hợp đồng envelope."""


def to_utc_iso(value: datetime) -> str:
    """Đổi datetime có múi giờ sang chuỗi ISO 8601 UTC, ví dụ `2026-10-18T13:24:50Z`."""
    if value.tzinfo is None:
        raise EnvelopeError("timestamp phải có múi giờ")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def make_envelope(
    source: str,
    record_id: str,
    event_time: datetime,
    payload: dict[str, Any],
    ingest_time: datetime | None = None,
) -> dict[str, Any]:
    if source not in SOURCES:
        raise EnvelopeError(f"source không hợp lệ: {source!r}")
    if not record_id:
        raise EnvelopeError("record_id rỗng")
    return {
        "source": source,
        "record_id": str(record_id),
        "event_time": to_utc_iso(event_time),
        "ingest_time": to_utc_iso(ingest_time or datetime.now(UTC)),
        "schema_version": SCHEMA_VERSION,
        "payload": payload,
    }


def validate_envelope(record: Any) -> dict[str, Any]:
    """Kiểm nhanh một bản ghi đọc từ Kafka; bản ghi lỗi nên được chuyển vào dlq.raw."""
    if not isinstance(record, dict):
        raise EnvelopeError("bản ghi không phải object")
    missing = [f for f in REQUIRED_FIELDS if f not in record]
    if missing:
        raise EnvelopeError(f"thiếu trường: {', '.join(missing)}")
    if record["source"] not in SOURCES:
        raise EnvelopeError(f"source không hợp lệ: {record['source']!r}")
    if not isinstance(record["payload"], dict):
        raise EnvelopeError("payload phải là object")
    for field in ("event_time", "ingest_time"):
        value = record[field]
        if not isinstance(value, str) or not value.endswith("Z"):
            raise EnvelopeError(f"{field} phải là chuỗi UTC kết thúc bằng Z")
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    return record


def kafka_key(envelope: dict[str, Any]) -> str:
    return f"{envelope['source']}:{envelope['record_id']}"
