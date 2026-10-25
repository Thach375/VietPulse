"""Raw persistence keeps Kafka coordinates and even malformed envelope bytes."""

import json
from datetime import UTC, datetime

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import Row, SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from jobs.raw_ingest import JobConfig, prepare_records, write_raw  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-raw-ingest")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_preserves_invalid_values_and_kafka_coordinates(spark):
    now = datetime(2026, 10, 25, 3, 4, tzinfo=UTC)
    source = spark.createDataFrame(
        [
            Row(
                topic="raw.news.articles",
                partition=1,
                offset=25,
                key=b"source:id",
                value=b"{bad",
                timestamp=now,
            ),
            Row(topic="raw.backfill", partition=2, offset=26, key=None, value=None, timestamp=now),
        ]
    )
    records = prepare_records(source).orderBy("offset").collect()
    assert records[0].record_key == "source:id"
    assert records[0].raw_value == "{bad"
    assert (records[0].topic, records[0].partition, records[0].offset) == ("raw.news.articles", 1, 25)
    assert records[0].ingest_date == "2026-10-25"
    assert records[1].raw_value is None


def test_rejects_shared_checkpoint_and_unknown_topics():
    with pytest.raises(ValueError):
        JobConfig(output_path="hdfs://host/raw", checkpoint_location="hdfs://host/raw/")
    with pytest.raises(ValueError):
        JobConfig(topics=("raw.news.articles", "dlq.raw"))


def test_file_sink_reuses_checkpoint_without_rewriting_committed_rows(spark, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    output = tmp_path / "raw"
    checkpoint = tmp_path / "checkpoint"
    schema = T.StructType(
        [
            T.StructField("topic", T.StringType()),
            T.StructField("partition", T.IntegerType()),
            T.StructField("offset", T.LongType()),
            T.StructField("key", T.StringType()),
            T.StructField("value", T.StringType()),
            T.StructField("timestamp", T.TimestampType()),
        ]
    )
    config = JobConfig(output_path=str(output), checkpoint_location=str(checkpoint), available_now=True)

    def put(name, offset):
        (source / name).write_text(
            json.dumps(
                {
                    "topic": "raw.news.articles",
                    "partition": 0,
                    "offset": offset,
                    "key": f"key:{offset}",
                    "value": "{bad",
                    "timestamp": "2026-10-25T03:04:00Z",
                }
            )
            + "\n"
        )

    def run_once():
        records = spark.readStream.schema(schema).json(str(source))
        assert write_raw(records, config).awaitTermination(90)

    put("first.json", 25)
    run_once()
    run_once()
    assert [row.offset for row in spark.read.parquet(str(output)).collect()] == [25]
    put("second.json", 26)
    run_once()
    assert sorted(row.offset for row in spark.read.parquet(str(output)).collect()) == [25, 26]
