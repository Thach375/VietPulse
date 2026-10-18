import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import jsonschema
import pytest

from common.envelope import EnvelopeError, kafka_key, make_envelope, validate_envelope

SCHEMA = json.loads((Path(__file__).parents[1] / "contracts" / "envelope.schema.json").read_text("utf-8"))


def _sample():
    return make_envelope(
        source="news_comment",
        record_id="vnexpress:123",
        event_time=datetime(2026, 10, 18, 20, 24, 50, tzinfo=timezone(timedelta(hours=7))),
        payload={"content": "Chuẩn luôn"},
        ingest_time=datetime(2026, 10, 18, 14, 0, tzinfo=UTC),
    )


def test_envelope_matches_contract_schema():
    jsonschema.validate(_sample(), SCHEMA)


def test_times_are_converted_to_utc():
    envelope = _sample()
    assert envelope["event_time"] == "2026-10-18T13:24:50Z"
    assert envelope["ingest_time"] == "2026-10-18T14:00:00Z"


def test_naive_datetime_is_rejected():
    with pytest.raises(EnvelopeError):
        make_envelope("voz_post", "voz:1", datetime(2026, 10, 18), {})


def test_unknown_source_is_rejected():
    with pytest.raises(EnvelopeError):
        make_envelope("facebook_post", "x:1", datetime.now(UTC), {})


def test_kafka_key_joins_source_and_record_id():
    assert kafka_key(_sample()) == "news_comment:vnexpress:123"


def test_validate_accepts_valid_record():
    assert validate_envelope(_sample())["record_id"] == "vnexpress:123"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda r: r.pop("payload"),
        lambda r: r.update(source="unknown"),
        lambda r: r.update(event_time="2026-10-18 13:24:50"),
        lambda r: r.update(payload="not an object"),
    ],
)
def test_validate_rejects_broken_records(mutate):
    record = _sample()
    mutate(record)
    with pytest.raises((EnvelopeError, ValueError)):
        validate_envelope(record)
