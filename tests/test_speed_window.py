"""The speed-window branch deduplicates before computing one fixed five-minute cell."""

import json
from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from common.envelope import make_envelope  # noqa: E402
from jobs.speed_window import WindowConfig, build_windows, prepare_deduplicated  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-speed-window")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_deduplicates_record_id_and_aggregates_tumbling_window(spark, tmp_path):
    event = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=10)
    envelope = make_envelope("news_comment", "same-id", event, {"site": "vnexpress", "content": "rất tốt"})
    source = tmp_path / "source"
    source.mkdir()
    (source / "records.json").write_text(
        "".join(
            json.dumps(
                {
                    "topic": "raw.news.comments",
                    "partition": 0,
                    "offset": offset,
                    "timestamp": event.isoformat(),
                    "value": json.dumps(envelope),
                }
            )
            + "\n"
            for offset in (1, 2)
        )
    )
    schema = T.StructType(
        [
            T.StructField("topic", T.StringType()),
            T.StructField("partition", T.IntegerType()),
            T.StructField("offset", T.LongType()),
            T.StructField("timestamp", T.TimestampType()),
            T.StructField("value", T.StringType()),
        ]
    )
    incoming = spark.readStream.schema(schema).json(str(source))
    deduplicated_stream = prepare_deduplicated(incoming)
    output = tmp_path / "dedup"
    query = (
        deduplicated_stream.writeStream.format("parquet")
        .option("path", str(output))
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .trigger(availableNow=True)
        .start()
    )
    assert query.awaitTermination(90)
    deduplicated = spark.read.parquet(str(output))
    assert [row.document_id for row in deduplicated.collect()] == ["news_comment:same-id"]
    rows = build_windows(deduplicated).collect()
    assert len(rows) == 1
    assert rows[0].total == 1
    assert rows[0].positive + rows[0].negative + rows[0].neutral == 1
    assert rows[0].source == "news_comment"
    assert rows[0].site == "vnexpress"
    assert rows[0].window.end - rows[0].window.start == timedelta(minutes=5)


def test_window_paths_must_not_overlap():
    with pytest.raises(ValueError):
        WindowConfig(output_path="/same", dedup_checkpoint="/same/", window_checkpoint="/different")
