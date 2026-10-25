"""Kiểm thử Kafka → query A → Elasticsearch/DLQ trên Compose bằng dữ liệu tổng hợp.

Chạy: docker compose exec spark spark-submit --master local[2] tests/integration/speed_sentiment_smoke.py
Tạo index, policy, template và checkpoint riêng; không xóa dữ liệu hay checkpoint đang có.
"""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.envelope import kafka_key, make_envelope
from jobs.setup_elasticsearch import request_json, setup_elasticsearch
from jobs.speed_sentiment import JobConfig, start_query


def main():
    run_id = f"smoke-{uuid4().hex[:12]}"
    spark = SparkSession.builder.appName("speed-sentiment-smoke").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    config = JobConfig(
        docs_index=f"vp-{run_id}-docs",
        dlq_index=f"vp-{run_id}-dlq",
        checkpoint_location=f"data/checkpoints/{run_id}",
        available_now=True,
    )

    def record(source, suffix, payload, days_old=0):
        return make_envelope(
            source, f"{run_id}:{suffix}", datetime.now(UTC) - timedelta(days=days_old), payload
        )

    comment = record("news_comment", "1", {"site": "test", "content": "ko ổn"})
    article = record("news_article", "1", {"site": "test", "title": "Rất tốt", "body": "😍"})
    voz = record("voz_post", "1", {"site": "test", "content": None})
    late = record("news_comment", "late", {"content": "Thật tuyệt vời"}, days_old=2)
    missing = {key: value for key, value in comment.items() if key != "payload"}
    future = {**comment, "event_time": "2099-01-01T00:00:00Z"}
    inputs = [
        (article, "raw.news.articles"),
        (comment, "raw.news.comments"),
        (comment, "raw.news.comments"),
        (voz, "raw.forum.posts"),
        (late, "raw.news.comments"),
        (missing, "raw.news.comments"),
        (future, "raw.news.comments"),
        (article, "raw.news.comments"),
    ]

    def publish(rows):
        spark.createDataFrame(rows, "key string, value string, topic string").write.format("kafka").option(
            "kafka.bootstrap.servers", config.bootstrap_servers
        ).save()

    def drain(job_config):
        query = start_query(spark, job_config)
        if not query.awaitTermination(180):
            query.stop()
            raise TimeoutError("Query chưa hoàn thành sau 180 giây")

    def documents():
        request_json(config.es_url, f"/{config.docs_index}/_refresh", method="POST")
        return request_json(
            config.es_url,
            f"/{config.docs_index}/_search",
            {"size": 20, "query": {"prefix": {"record_id": run_id}}},
            "POST",
        )["hits"]["hits"]

    def dlq_documents():
        request_json(config.es_url, f"/{config.dlq_index}/_refresh", method="POST")
        hits = request_json(config.es_url, f"/{config.dlq_index}/_search?size=10000")["hits"]["hits"]
        return [hit for hit in hits if run_id in (hit["_source"].get("raw_value") or "")]

    def kafka_errors():
        return (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", config.bootstrap_servers)
            .option("subscribe", "dlq.raw")
            .load()
            .filter(F.col("value").cast("string").contains(run_id))
            .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
            .collect()
        )

    try:
        setup_elasticsearch(config.es_url, config.docs_index, config.dlq_index)
        setup_elasticsearch(config.es_url, config.docs_index, config.dlq_index)
        rows = [(kafka_key(value), json.dumps(value, ensure_ascii=False), topic) for value, topic in inputs]
        rows.append((f"{run_id}:broken", f'{{"run":"{run_id}",broken', "raw.news.comments"))
        publish(rows)
        drain(config)

        docs = {
            hit["_source"]["source"] + ":" + hit["_source"]["record_id"]: hit["_source"]
            for hit in documents()
        }
        assert len(docs) == 4, docs
        assert docs[f"news_article:{run_id}:1"]["sentiment"]["label"] == "positive"
        assert docs[f"news_comment:{run_id}:1"]["sentiment"]["label"] == "negative"
        assert docs[f"voz_post:{run_id}:1"]["sentiment"]["label"] == "neutral"
        assert docs[f"news_comment:{run_id}:late"]["sentiment"]["label"] == "positive"
        assert len(dlq_documents()) == 4
        errors = kafka_errors()
        assert len(errors) == len({row.key for row in errors}) == 4
        assert all(json.loads(row.value)["error"] for row in errors)

        drain(config)
        assert len(documents()) == 4
        assert len(kafka_errors()) == 4, "Resume checkpoint không được gửi lại lỗi đã commit"

        drain(replace(config, checkpoint_location=f"data/checkpoints/{run_id}-replay"))
        assert len(documents()) == 4, "Replay không được nhân bản tài liệu"
        assert len(dlq_documents()) == 4, "Replay không được nhân bản DLQ Elasticsearch"
        assert len(kafka_errors()) == 8, "Kafka DLQ có ngữ nghĩa at-least-once"

        updated = {**comment, "payload": {"content": "Rất tốt"}}
        publish([(kafka_key(updated), json.dumps(updated, ensure_ascii=False), "raw.news.comments")])
        drain(config)
        docs = documents()
        assert len(docs) == 4
        updated_doc = next(hit["_source"] for hit in docs if hit["_id"] == f"news_comment:{run_id}:1")
        assert updated_doc["sentiment"]["label"] == "positive"

        ilm = request_json(config.es_url, f"/{config.dlq_index}/_ilm/explain")["indices"]
        assert all(
            index["managed"] and index["policy"] == f"{config.dlq_index}-retention" for index in ilm.values()
        )
        print(f"M2.3 OK ({run_id}): 4 tài liệu, 4 DLQ; resume, replay, cập nhật và ILM đều đúng")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
