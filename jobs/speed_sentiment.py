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

    def es_options(self):
        target = urlsplit(self.es_url)
        return {
            "es.nodes": target.hostname,
            "es.port": str(target.port or (443 if target.scheme == "https" else 9200)),
            "es.net.ssl": str(target.scheme == "https").lower(),
            "es.nodes.wan.only": "true",
            "es.mapping.id": "document_id",
            "es.spark.dataframe.write.null": "true",
            "es.write.operation": "index",
            "es.index.auto.create": "false",
        }


def write_documents(documents: DataFrame, config: JobConfig):
    documents.write.format("es").mode("append").options(**config.es_options()).save(config.docs_index)


def write_dlq(errors: DataFrame, config: JobConfig):
    # Ngày Kafka ổn định qua retry, không đổi index nếu batch được chạy lại sang ngày khác.
    indexed = errors.withColumn("index_day", F.date_format("kafka_timestamp", "yyyy.MM.dd"))
    options = {**config.es_options(), "es.index.auto.create": "true", "es.mapping.exclude": "index_day"}
    indexed.write.format("es").mode("append").options(**options).save(f"{config.dlq_index}-{{index_day}}")
    fields = [name for name in errors.columns if name != "document_id"]
    errors.select(
        F.col("document_id").alias("key"),
        F.to_json(F.struct(*fields), {"ignoreNullFields": "false"}).alias("value"),
    ).write.format("kafka").option("kafka.bootstrap.servers", config.bootstrap_servers).option(
        "topic", "dlq.raw"
    ).save()


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
    args = vars(parser.parse_args(argv))
    args["topics"] = tuple(topic.strip() for topic in args["topics"].split(",") if topic.strip())
    return JobConfig(**args)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = parse_args()
    spark = SparkSession.builder.appName("vietpulse-speed-sentiment").getOrCreate()
    try:
        start_query(spark, config).awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
