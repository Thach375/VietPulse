import json
from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from common.envelope import make_envelope  # noqa: E402
from common.spark_processing import build_dlq, build_documents, parse_kafka_records  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-speed-sentiment")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def envelope(source="news_comment", record_id="vnexpress:1", payload=None):
    return make_envelope(source, record_id, datetime.now(UTC) - timedelta(days=2), payload or {})


def kafka_rows(spark, values):
    return spark.createDataFrame(
        [(value, topic, 0, offset, datetime(2026, 10, 25)) for offset, (value, topic) in enumerate(values)],
        "value string, topic string, partition int, offset long, timestamp timestamp",
    )


def test_sql_validation_routes_bad_records_without_a_python_udf(spark):
    good = envelope()
    cases = [
        (json.dumps(good), "raw.news.comments", None),
        ("{broken", "raw.news.comments", "JSON"),
        ("[]", "raw.news.comments", "JSON"),
        ("null", "raw.news.comments", "JSON"),
        (None, "raw.news.comments", "JSON"),
        (
            json.dumps({key: value for key, value in good.items() if key != "payload"}),
            "raw.news.comments",
            "thiếu trường",
        ),
        (json.dumps({**good, "source": "unknown"}), "raw.news.comments", "source không hợp lệ"),
        (json.dumps(good), "raw.news.articles", "source không khớp topic"),
        (json.dumps({**good, "record_id": " "}), "raw.news.comments", "record_id"),
        *[
            (json.dumps({**good, "record_id": value}), "raw.news.comments", "record_id")
            for value in (123, True, {}, [], None)
        ],
        (json.dumps({**good, "schema_version": "1"}), "raw.news.comments", "schema_version"),
        (json.dumps({**good, "schema_version": 0}), "raw.news.comments", "schema_version"),
        (json.dumps({**good, "payload": []}), "raw.news.comments", "payload"),
        (json.dumps({**good, "payload": "{}"}), "raw.news.comments", "payload"),
        (json.dumps({**good, "event_time": "2026-02-30T00:00:00Z"}), "raw.news.comments", "event_time"),
        (json.dumps({**good, "event_time": "2026-10-25T00:00:00+00:00"}), "raw.news.comments", "event_time"),
        (json.dumps({**good, "ingest_time": None}), "raw.news.comments", "ingest_time"),
        (json.dumps({**good, "event_time": "2099-01-01T00:00:00Z"}), "raw.news.comments", "tương lai"),
    ]
    parsed = parse_kafka_records(kafka_rows(spark, [(value, topic) for value, topic, _ in cases]))
    assert "PythonUDF" not in parsed._jdf.queryExecution().optimizedPlan().toString()
    errors = {row.offset: row.error for row in parsed.collect()}
    for offset, (_, _, expected) in enumerate(cases):
        if expected is None:
            assert errors[offset] is None
        else:
            assert errors[offset] is not None and expected in errors[offset], (offset, errors[offset])


def test_documents_reuse_common_sentiment_and_preserve_late_empty_and_nested_payloads(spark):
    records = [
        (
            envelope("news_article", payload={"title": "<b>Rất tốt</b>", "body": "😍", "extra": {"n": 1}}),
            "raw.news.articles",
        ),
        (envelope(payload={"content": "ko ổn"}), "raw.news.comments"),
        (envelope("voz_post", "voz:1", {"content": None}), "raw.forum.posts"),
    ]
    parsed = parse_kafka_records(
        kafka_rows(spark, [(json.dumps(record), topic) for record, topic in records])
    )
    assert all(row.error is None for row in parsed.collect())
    docs = {row.source: row for row in build_documents(parsed).collect()}
    assert docs["news_article"].sentiment.label == "positive"
    assert docs["news_article"].sentiment.model == "rules-v1"
    assert json.loads(docs["news_article"].payload_json)["extra"] == {"n": 1}
    assert docs["news_article"].normalized_text == "Rất tốt 😍"
    assert docs["news_comment"].sentiment.label == "negative"
    assert docs["news_comment"].normalized_text == "không ổn"
    assert docs["voz_post"].normalized_text == ""
    assert docs["voz_post"].sentiment.score == 0.0
    assert len({row.document_id for row in docs.values()}) == 3


def test_dlq_retains_raw_value_and_kafka_position(spark):
    parsed = parse_kafka_records(kafka_rows(spark, [("{broken", "raw.news.comments")]))
    [row] = build_dlq(parsed).collect()
    assert row.document_id == "raw.news.comments:0:0"
    assert row.raw_value == "{broken"
    assert row.error
    assert row.kafka_timestamp == datetime(2026, 10, 25)
    assert row.detected_at is not None


def test_documents_choose_latest_ingest_in_batch_with_stable_offset_tiebreak(spark):
    old = {**envelope(payload={"content": "Tệ"}), "ingest_time": "2026-10-24T00:00:00Z"}
    new = {**old, "payload": {"content": "Rất tốt"}, "ingest_time": "2026-10-25T00:00:00Z"}
    # Offset cao hơn nhưng ingest_time cũ hơn không được ghi đè phiên bản mới.
    values = [(json.dumps(value), "raw.news.comments") for value in (old, new, old, new)]
    [document] = build_documents(parse_kafka_records(kafka_rows(spark, values))).collect()
    assert document.sentiment.label == "positive"
    assert document.content == "Rất tốt"


def test_sink_failure_propagates_and_releases_cached_batch(spark, monkeypatch):
    from jobs import speed_sentiment as job

    records = kafka_rows(spark, [(json.dumps(envelope()), "raw.news.comments")])
    parsed = parse_kafka_records(records)
    monkeypatch.setattr(job, "parse_kafka_records", lambda records: parsed)

    def fail(documents, config):
        raise OSError("Elasticsearch unavailable")

    monkeypatch.setattr(job, "write_documents", fail)
    with pytest.raises(OSError, match="Elasticsearch unavailable"):
        job.process_batch(records, 0, job.JobConfig())
    assert not parsed.is_cached
