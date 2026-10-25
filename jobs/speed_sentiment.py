"""Query A: Kafka → sentiment bằng luật → Elasticsearch, Kafka DLQ.

Chạy: spark-submit jobs/speed_sentiment.py --checkpoint-location data/checkpoints/speed-sentiment-documents
Mapping cần được tạo trước bằng jobs.setup_elasticsearch.
"""

import argparse
import logging
from dataclasses import dataclass
from urllib.parse import urlsplit

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.spark_processing import (
    DEFAULT_TOPICS,
    SOURCE_TOPICS,
    build_dlq,
    build_documents,
    parse_kafka_records,
)
from jobs.es_http import bulk_index_partition
from jobs.setup_elasticsearch import setup_elasticsearch
from jobs.speed_window import WindowConfig, start_dedup_query, start_window_query

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobConfig:
    bootstrap_servers: str = "kafka:9092"
    topics: tuple[str, ...] = DEFAULT_TOPICS
    checkpoint_location: str = "data/checkpoints/speed-sentiment-documents"
    starting_offsets: str = "earliest"
    trigger_interval: str = "30 seconds"
    max_offsets_per_trigger: int = 10000
    es_url: str = "http://elasticsearch:9200"
    docs_index: str = "vp-docs"
    dlq_index: str = "vp-dlq"
    available_now: bool = False
    enable_windows: bool = False
    dedup_output_path: str = WindowConfig.output_path
    dedup_checkpoint: str = WindowConfig.dedup_checkpoint
    window_checkpoint: str = WindowConfig.window_checkpoint
    mongo_uri: str = WindowConfig.mongo_uri
    mongo_password_file: str = WindowConfig.mongo_password_file

    def __post_init__(self):
        if not self.checkpoint_location.strip():
            raise ValueError("checkpoint_location không được rỗng")
        if not self.topics or set(self.topics) - set(SOURCE_TOPICS.values()):
            raise ValueError("chỉ đọc topic raw của các source; không đọc backfill, DLQ hay alerts")
        if self.max_offsets_per_trigger < 1:
            raise ValueError("max_offsets_per_trigger phải lớn hơn 0")
        target = urlsplit(self.es_url)
        if target.scheme not in ("http", "https") or not target.hostname or target.path not in ("", "/"):
            raise ValueError("es_url phải là http(s)://host[:port]")
        if target.username or target.password or target.query or target.fragment:
            raise ValueError("es_url không chứa thông tin đăng nhập, query hay fragment")
        if self.enable_windows:
            self.window_config()

    def window_config(self) -> WindowConfig:
        return WindowConfig(
            output_path=self.dedup_output_path,
            dedup_checkpoint=self.dedup_checkpoint,
            window_checkpoint=self.window_checkpoint,
            mongo_uri=self.mongo_uri,
            mongo_password_file=self.mongo_password_file,
            available_now=self.available_now,
        )


def write_documents(documents: DataFrame, config: JobConfig):
    documents.foreachPartition(lambda rows: bulk_index_partition(rows, config.es_url, config.docs_index))


def write_dlq(errors: DataFrame, config: JobConfig):
    # Ngày Kafka ổn định qua retry, không đổi index nếu batch được chạy lại sang ngày khác.
    indexed = errors.withColumn("index_day", F.date_format("kafka_timestamp", "yyyy.MM.dd"))
    indexed.foreachPartition(
        lambda rows: bulk_index_partition(rows, config.es_url, config.dlq_index, "index_day")
    )
    fields = [name for name in errors.columns if name != "document_id"]
    errors.select(
        F.col("document_id").alias("key"),
        F.to_json(F.struct(*fields), {"ignoreNullFields": "false"}).alias("value"),
    ).write.format("kafka").option("kafka.bootstrap.servers", config.bootstrap_servers).option(
        "topic", "dlq.raw"
    ).option("kafka.acks", "all").option("kafka.enable.idempotence", "true").save()


def process_batch(records: DataFrame, batch_id: int, config: JobConfig):
    parsed = parse_kafka_records(records).persist()
    try:
        counts = {
            row.valid: row["count"]
            for row in parsed.groupBy(F.col("error").isNull().alias("valid")).count().collect()
        }
        if counts.get(True, 0):
            write_documents(build_documents(parsed.filter(F.col("error").isNull())), config)
        if counts.get(False, 0):
            write_dlq(build_dlq(parsed.filter(F.col("error").isNotNull())), config)
        LOGGER.info(
            "batch=%s valid_records=%s dlq_records=%s", batch_id, counts.get(True, 0), counts.get(False, 0)
        )
    finally:
        # Để lỗi sink truyền tới Spark: checkpoint không được đánh dấu batch đã xong nếu còn lỗi ghi.
        parsed.unpersist()


def read_records(spark: SparkSession, config: JobConfig):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", ",".join(config.topics))
        .option("startingOffsets", config.starting_offsets)
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
        .load()
    )


def start_query(spark: SparkSession, config: JobConfig):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    records = read_records(spark, config)
    writer = (
        records.writeStream.queryName("speed-sentiment-documents")
        .outputMode("append")
        .option("checkpointLocation", config.checkpoint_location)
        .foreachBatch(lambda batch, batch_id: process_batch(batch, batch_id, config))
    )
    writer = (
        writer.trigger(availableNow=True)
        if config.available_now
        else writer.trigger(processingTime=config.trigger_interval)
    )
    return writer.start()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=JobConfig.bootstrap_servers)
    parser.add_argument("--topics", default=",".join(DEFAULT_TOPICS))
    parser.add_argument("--checkpoint-location", default=JobConfig.checkpoint_location)
    parser.add_argument("--starting-offsets", choices=("earliest", "latest"), default="earliest")
    parser.add_argument("--trigger-interval", default=JobConfig.trigger_interval)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=JobConfig.max_offsets_per_trigger)
    parser.add_argument("--es-url", default=JobConfig.es_url)
    parser.add_argument("--docs-index", default=JobConfig.docs_index)
    parser.add_argument("--dlq-index", default=JobConfig.dlq_index)
    parser.add_argument(
        "--available-now", action="store_true", help="xử lý backlog rồi thoát, giữ checkpoint để chạy tiếp"
    )
    parser.add_argument("--enable-windows", action="store_true")
    parser.add_argument("--dedup-output-path", default=WindowConfig.output_path)
    parser.add_argument("--dedup-checkpoint", default=WindowConfig.dedup_checkpoint)
    parser.add_argument("--window-checkpoint", default=WindowConfig.window_checkpoint)
    parser.add_argument("--mongo-uri", default=WindowConfig.mongo_uri)
    parser.add_argument("--mongo-password-file", default=WindowConfig.mongo_password_file)
    args = vars(parser.parse_args(argv))
    args["topics"] = tuple(topic.strip() for topic in args["topics"].split(",") if topic.strip())
    return JobConfig(**args)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = parse_args()
    spark = SparkSession.builder.appName("vietpulse-speed-sentiment").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    queries = []
    try:
        setup_elasticsearch(config.es_url, config.docs_index, config.dlq_index)
        documents = start_query(spark, config)
        queries.append(documents)
        if config.enable_windows:
            window_config = config.window_config()
            dedup = start_dedup_query(read_records(spark, config), window_config)
            queries.append(dedup)
            if config.available_now:
                documents.awaitTermination()
                dedup.awaitTermination()
            windows = start_window_query(spark, window_config)
            queries.append(windows)
        if config.available_now:
            for query in queries:
                query.awaitTermination()
        else:
            spark.streams.awaitAnyTermination()
            raise RuntimeError("A continuous speed-sentiment query terminated unexpectedly")
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
