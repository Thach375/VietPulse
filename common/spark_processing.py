"""Biểu thức Spark dùng chung cho speed layer và batch-clean.

Kiểm envelope bằng SQL; chỉ chuẩn hóa và chấm sentiment mới chạy Python UDF.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.envelope import REQUIRED_FIELDS, SOURCES
from common.text_processing import analyze_sentiment, normalize_text

SOURCE_TOPICS = {
    "news_article": "raw.news.articles",
    "news_comment": "raw.news.comments",
    "voz_post": "raw.forum.posts",
    "youtube_comment": "raw.youtube.comments",
}
DEFAULT_TOPICS = tuple(topic for source, topic in SOURCE_TOPICS.items() if source != "youtube_comment")
UTC_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$"
ENVELOPE_SCHEMA = T.StructType(
    [T.StructField(name, T.StringType()) for name in ("source", "record_id", "event_time", "ingest_time")]
    + [
        T.StructField("schema_version", T.LongType()),
        T.StructField("payload", T.MapType(T.StringType(), T.StringType())),
        T.StructField("_corrupt_record", T.StringType()),
    ]
)
JSON_OPTIONS = {"allowSingleQuotes": "false", "allowNonNumericNumbers": "false"}


def parse_kafka_records(records: DataFrame, allow_backfill: bool = False) -> DataFrame:
    """Giữ chuỗi gốc và vị trí Kafka, thêm envelope đã parse cùng lý do lỗi (hoặc null)."""
    parsed = records.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("source_topic"),
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
    ).withColumn("envelope", F.from_json("raw_value", ENVELOPE_SCHEMA, JSON_OPTIONS))
    parsed = parsed.withColumns(
        {
            "event_time": F.try_to_timestamp("envelope.event_time"),
            "ingest_time": F.try_to_timestamp("envelope.ingest_time"),
            "detected_at": F.current_timestamp(),
        }
    )
    fields = F.json_object_keys("raw_value")
    missing = F.array_except(F.array(*[F.lit(name) for name in REQUIRED_FIELDS]), fields)
    topic_map = F.create_map(*[F.lit(value) for pair in SOURCE_TOPICS.items() for value in pair])
    # Wildcard trên mảng bọc ngoài giữ dấu nháy JSON; StringType tự ép số/bool thành chuỗi.
    record_id_json = F.get_json_object(F.concat(F.lit("["), F.col("raw_value"), F.lit("]")), "$[*].record_id")
    error = (
        F.when(fields.isNull(), "JSON không hợp lệ hoặc bản ghi không phải object")
        .when(F.size(missing) > 0, F.concat(F.lit("thiếu trường: "), F.concat_ws(", ", missing)))
        .when(
            ~F.coalesce(F.col("envelope.source").isin(sorted(SOURCES)), F.lit(False)), "source không hợp lệ"
        )
        .when(
            (F.col("source_topic") != topic_map[F.col("envelope.source")])
            & ~(F.lit(allow_backfill) & (F.col("source_topic") == "raw.backfill")),
            "source không khớp topic",
        )
        .when(
            F.col("envelope.record_id").isNull()
            | ~record_id_json.startswith('"')
            | (F.length(F.trim("envelope.record_id")) == 0),
            "record_id rỗng hoặc không hợp lệ",
        )
        .when(
            F.col("envelope.schema_version").isNull() | (F.col("envelope.schema_version") < 1),
            "schema_version phải là số nguyên dương",
        )
        .when(F.col("envelope.payload").isNull(), "payload phải là object")
    )
    for name in ("event_time", "ingest_time"):
        error = error.when(
            F.col(name).isNull() | ~F.col(f"envelope.{name}").rlike(UTC_PATTERN),
            f"{name} phải là timestamp UTC hợp lệ kết thúc bằng Z",
        )
    return parsed.withColumn(
        "error",
        error.when(
            F.col("event_time") > F.col("detected_at") + F.expr("INTERVAL 1 HOUR"),
            "event_time ở tương lai quá 1 giờ",
        ).when(F.col("envelope._corrupt_record").isNotNull(), "JSON hoặc kiểu dữ liệu envelope không hợp lệ"),
    )


@F.udf(
    T.StructType(
        [
            T.StructField("normalized_text", T.StringType()),
            T.StructField(
                "sentiment",
                T.StructType(
                    [
                        T.StructField("label", T.StringType()),
                        T.StructField("score", T.DoubleType()),
                        T.StructField("model", T.StringType()),
                    ]
                ),
            ),
        ]
    )
)
def _score_text(text):
    normalized = normalize_text(text)
    return {
        "normalized_text": normalized,
        "sentiment": {**analyze_sentiment(text), "model": "rules-v1"},
    }


def build_documents(valid: DataFrame) -> DataFrame:
    """Tạo tài liệu tìm kiếm; khóa ghép tránh trùng id bài báo với id bình luận cùng trang."""
    # Chọn bản mới nhất trong batch để các task ghi song song không đua nhau trên cùng _id.
    latest = Window.partitionBy("envelope.source", "envelope.record_id").orderBy(
        F.col("ingest_time").desc(),
        F.col("kafka_timestamp").desc(),
        F.col("partition").desc(),
        F.col("offset").desc(),
    )
    valid = valid.withColumn("_rank", F.row_number().over(latest)).filter(F.col("_rank") == 1)
    payload = F.col("envelope.payload")
    text = F.when(
        F.col("envelope.source") == "news_article",
        F.concat_ws("\n", payload["title"], payload["summary"], payload["body"]),
    ).otherwise(F.coalesce(payload["content"], F.lit("")))
    documents = valid.withColumn("analysis", _score_text(text))
    searchable = (
        "site",
        "url",
        "category",
        "title",
        "summary",
        "body",
        "content",
        "article_id",
        "thread_id",
        "thread_title",
    )
    return documents.select(
        F.concat_ws(":", "envelope.source", "envelope.record_id").alias("document_id"),
        "envelope.source",
        "envelope.record_id",
        "event_time",
        "ingest_time",
        "envelope.schema_version",
        F.get_json_object("raw_value", "$.payload").alias("payload_json"),
        *[payload[name].alias(name) for name in searchable],
        "analysis.normalized_text",
        "analysis.sentiment",
    )


def build_dlq(invalid: DataFrame) -> DataFrame:
    """Khóa DLQ ổn định theo vị trí Kafka để retry Elasticsearch không nhân bản lỗi."""
    return invalid.select(
        F.concat_ws(":", "source_topic", "partition", "offset").alias("document_id"),
        "raw_value",
        "error",
        "source_topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "detected_at",
    )
