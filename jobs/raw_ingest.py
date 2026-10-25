"""Kafka raw topics → HDFS Parquet, preserving every envelope and malformed value.

The file sink and its dedicated HDFS checkpoint commit each micro-batch together.
https://spark.apache.org/docs/4.0.4/streaming/apis-on-dataframes-and-datasets.html
"""

import argparse
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.spark_processing import DEFAULT_TOPICS

RAW_TOPICS = (*DEFAULT_TOPICS, "raw.backfill")


@dataclass(frozen=True)
class JobConfig:
    bootstrap_servers: str = "vietpulse-kafka-bootstrap.messaging.svc:9092"
    topics: tuple[str, ...] = RAW_TOPICS
    output_path: str = "hdfs://hdfs-namenode.storage.svc.cluster.local:8020/vietpulse/raw"
    checkpoint_location: str = (
        "hdfs://hdfs-namenode.storage.svc.cluster.local:8020/vietpulse/checkpoints/raw-ingest"
    )
    starting_offsets: str = "earliest"
    trigger_interval: str = "10 minutes"
    max_offsets_per_trigger: int = 100000
    available_now: bool = False

    def __post_init__(self):
        if not self.bootstrap_servers or not self.output_path or not self.checkpoint_location:
            raise ValueError("Kafka, HDFS output and checkpoint addresses are required")
        if self.output_path.rstrip("/") == self.checkpoint_location.rstrip("/"):
            raise ValueError("The raw output and checkpoint directories must differ")
        if not self.topics or len(set(self.topics)) != len(self.topics) or set(self.topics) - set(RAW_TOPICS):
            raise ValueError("raw-ingest accepts only distinct configured raw topics")
        if self.max_offsets_per_trigger < 1:
            raise ValueError("max_offsets_per_trigger must be positive")


def prepare_records(records: DataFrame) -> DataFrame:
    """Retain Kafka coordinates so batch-clean can reconcile and reject bad records."""
    return records.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("key").cast("string").alias("record_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.date_format("timestamp", "yyyy-MM-dd").alias("ingest_date"),
    )


def start_query(spark: SparkSession, config: JobConfig):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    records = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", ",".join(config.topics))
        .option("startingOffsets", config.starting_offsets)
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
        .load()
    )
    return write_raw(records, config)


def write_raw(records: DataFrame, config: JobConfig):
    writer = (
        prepare_records(records)
        .writeStream.queryName("raw-ingest")
        .format("parquet")
        .outputMode("append")
        .partitionBy("topic", "ingest_date")
        .option("path", config.output_path)
        .option("checkpointLocation", config.checkpoint_location)
    )
    writer = (
        writer.trigger(availableNow=True)
        if config.available_now
        else writer.trigger(processingTime=config.trigger_interval)
    )
    return writer.start()


def parse_args(argv=None) -> JobConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=JobConfig.bootstrap_servers)
    parser.add_argument("--topics", default=",".join(RAW_TOPICS))
    parser.add_argument("--output-path", default=JobConfig.output_path)
    parser.add_argument("--checkpoint-location", default=JobConfig.checkpoint_location)
    parser.add_argument("--starting-offsets", choices=("earliest", "latest"), default="earliest")
    parser.add_argument("--trigger-interval", default=JobConfig.trigger_interval)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=JobConfig.max_offsets_per_trigger)
    parser.add_argument("--available-now", action="store_true")
    args = vars(parser.parse_args(argv))
    args["topics"] = tuple(topic.strip() for topic in args["topics"].split(",") if topic.strip())
    return JobConfig(**args)


def main():
    config = parse_args()
    spark = SparkSession.builder.appName("vietpulse-raw-ingest").getOrCreate()
    try:
        start_query(spark, config).awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
