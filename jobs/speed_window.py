"""Deduplicate events, then aggregate fixed sentiment windows in separate Spark queries.

Spark 4.0.4 forbids chaining stateful deduplication and aggregation in Update mode.
https://spark.apache.org/docs/4.0.4/streaming/apis-on-dataframes-and-datasets.html#unsupported-operations
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.spark_processing import _score_text, parse_kafka_records

DEDUP_SCHEMA = T.StructType(
    [
        T.StructField("document_id", T.StringType(), False),
        T.StructField("source", T.StringType(), False),
        T.StructField("site", T.StringType(), True),
        T.StructField("event_time", T.TimestampType(), False),
        T.StructField("text", T.StringType(), True),
    ]
)


@dataclass(frozen=True)
class WindowConfig:
    output_path: str = "hdfs://hdfs-namenode.storage.svc.cluster.local:8020/vietpulse/checkpoints/speed-sentiment-dedup-output"
    dedup_checkpoint: str = (
        "hdfs://hdfs-namenode.storage.svc.cluster.local:8020/vietpulse/checkpoints/speed-sentiment-b-dedup"
    )
    window_checkpoint: str = (
        "hdfs://hdfs-namenode.storage.svc.cluster.local:8020/vietpulse/checkpoints/speed-sentiment-b"
    )

    def __post_init__(self):
        paths = (self.output_path, self.dedup_checkpoint, self.window_checkpoint)
        if not all(paths) or len({path.rstrip("/") for path in paths}) != 3:
            raise ValueError("dedup data and both streaming checkpoints require distinct paths")


def prepare_deduplicated(records: DataFrame) -> DataFrame:
    valid = parse_kafka_records(records).filter(F.col("error").isNull())
    payload = F.col("envelope.payload")
    content = F.when(
        F.col("envelope.source") == "news_article",
        F.concat_ws("\n", payload["title"], payload["summary"], payload["body"]),
    ).otherwise(F.coalesce(payload["content"], F.lit("")))
    prepared = valid.select(
        F.concat_ws(":", "envelope.source", "envelope.record_id").alias("document_id"),
        F.col("envelope.source").alias("source"),
        payload["site"].alias("site"),
        "event_time",
        content.alias("text"),
    )
    # One stateful operation. Checkpoint must persist across pod restarts.
    return prepared.withWatermark("event_time", "6 hours").dropDuplicatesWithinWatermark(["document_id"])


def build_windows(deduplicated: DataFrame) -> DataFrame:
    scored = deduplicated.withColumn("sentiment", _score_text(F.col("text")).getField("sentiment"))
    # This second query has only window aggregation as its stateful operation.
    return (
        scored.withWatermark("event_time", "6 hours")
        .groupBy(F.window("event_time", "5 minutes"), "source", "site")
        .agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.col("sentiment.label") == "positive", 1).otherwise(0)).alias("positive"),
            F.sum(F.when(F.col("sentiment.label") == "negative", 1).otherwise(0)).alias("negative"),
            F.sum(F.when(F.col("sentiment.label") == "neutral", 1).otherwise(0)).alias("neutral"),
            F.sum(F.col("sentiment.score")).alias("score_sum"),
        )
    )
