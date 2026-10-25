import json
from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from common.envelope import make_envelope  # noqa: E402
from common.spark_processing import parse_kafka_records  # noqa: E402
from jobs.batch import clean_records, daily_metrics, reference_digest  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-batch")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_backfill_and_late_records_are_cleaned_with_shared_rules(spark):
    event = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=3)
    record = make_envelope("news_comment", "one", event, {"content": "rất tốt", "site": "vnexpress"})
    raw = spark.createDataFrame(
        [
            (json.dumps(record), "raw.backfill", 0, 1, event),
            (json.dumps(record), "raw.news.comments", 0, 2, event),
            ("{broken", "raw.news.comments", 0, 3, event),
        ],
        "raw_value string, topic string, partition int, offset long, kafka_timestamp timestamp",
    )
    boundary = event + timedelta(days=1)
    clean = clean_records(raw, boundary)
    rows = clean.collect()
    assert len(rows) == 1
    assert rows[0].document_id == "news_comment:one"
    metrics = daily_metrics(clean).first()
    assert metrics.total == metrics.positive == 1
    assert metrics.negative == metrics.neutral == 0
    live = raw.filter("topic='raw.backfill'").selectExpr(
        "raw_value AS value", "topic", "partition", "offset", "kafka_timestamp AS timestamp"
    )
    assert parse_kafka_records(live).first().error == "source không khớp topic"


def test_reference_approval_digest_covers_exact_candidate_contents():
    first = [{"term": "báo", "count": 3}, {"term": "tin", "count": 2}]
    assert reference_digest(first) == reference_digest(list(reversed(first)))
    assert reference_digest(first) != reference_digest([{"term": "báo", "count": 4}])
