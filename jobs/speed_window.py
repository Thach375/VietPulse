"""Deduplicate events, then aggregate fixed sentiment windows in separate Spark queries.

Spark 4.0.4 forbids chaining stateful deduplication and aggregation in Update mode.
https://spark.apache.org/docs/4.0.4/streaming/apis-on-dataframes-and-datasets.html#unsupported-operations
MongoDB receives absolute window counts via deterministic replacements, so a
retried foreachBatch does not increment the same records twice.
https://www.mongodb.com/docs/languages/python/pymongo-driver/current/crud/bulk-write/
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pymongo import MongoClient, ReplaceOne, WriteConcern
from pyspark.sql import DataFrame, SparkSession
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
    mongo_uri: str = "mongodb://vietpulse-mongodb-svc.serving.svc.cluster.local:27017/?directConnection=true"
    mongo_password_file: str = "/var/run/vietpulse/mongo/password"
    available_now: bool = False

    def __post_init__(self):
        paths = (self.output_path, self.dedup_checkpoint, self.window_checkpoint)
        if not all(paths) or len({path.rstrip("/") for path in paths}) != 3:
            raise ValueError("dedup data and both streaming checkpoints require distinct paths")
        if not self.mongo_uri.startswith("mongodb://") or not self.mongo_password_file:
            raise ValueError("MongoDB address and mounted password file are required")


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


def window_document(row, batch_id: int) -> dict:
    """Store absolute counts and a stable key for every source/site/window cell."""
    start: datetime = row.window.start.replace(tzinfo=UTC)
    end: datetime = row.window.end.replace(tzinfo=UTC)
    source = row.source
    site = row.site or ""
    return {
        "_id": f"{start.isoformat()}|{source}|{site}",
        "window_start": start,
        "window_end": end,
        "source": source,
        "site": site,
        "total": row.total,
        "positive": row.positive,
        "negative": row.negative,
        "neutral": row.neutral,
        "score_sum": row.score_sum,
        "batch_id": batch_id,
    }


def write_window_batch(windows: DataFrame, batch_id: int, config: WindowConfig):
    def write_partition(rows):
        password = Path(config.mongo_password_file).read_text()
        with MongoClient(
            config.mongo_uri,
            username="vietpulse",
            password=password,
            authSource="admin",
            serverSelectionTimeoutMS=10000,
        ) as client:
            collection = client["vietpulse"].get_collection(
                "rt_window", write_concern=WriteConcern(w="majority", j=True)
            )
            operations = []
            for row in rows:
                document = window_document(row, batch_id)
                operations.append(ReplaceOne({"_id": document["_id"]}, document, upsert=True))
                if len(operations) == 100:
                    collection.bulk_write(operations, ordered=False)
                    operations.clear()
            if operations:
                collection.bulk_write(operations, ordered=False)

    windows.foreachPartition(write_partition)


def start_dedup_query(records: DataFrame, config: WindowConfig):
    writer = (
        prepare_deduplicated(records)
        .writeStream.queryName("speed-sentiment-dedup")
        .format("parquet")
        .outputMode("append")
        .option("path", config.output_path)
        .option("checkpointLocation", config.dedup_checkpoint)
    )
    writer = (
        writer.trigger(availableNow=True)
        if config.available_now
        else writer.trigger(processingTime="30 seconds")
    )
    return writer.start()


def start_window_query(spark: SparkSession, config: WindowConfig):
    deduplicated = spark.readStream.schema(DEDUP_SCHEMA).parquet(config.output_path)
    writer = (
        build_windows(deduplicated)
        .writeStream.queryName("speed-sentiment-window")
        .outputMode("update")
        .option("checkpointLocation", config.window_checkpoint)
        .foreachBatch(lambda batch, batch_id: write_window_batch(batch, batch_id, config))
    )
    writer = (
        writer.trigger(availableNow=True)
        if config.available_now
        else writer.trigger(processingTime="30 seconds")
    )
    return writer.start()
