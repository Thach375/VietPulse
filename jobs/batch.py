"""Sequential batch stages for the initial sentiment pipeline.

The demo rebuilds complete UTC-date partitions from all retained raw data so
late arrivals are included. Only publish changes the serving boundary T, after
Elasticsearch succeeds and MongoDB commits its daily views in one transaction.
https://www.mongodb.com/docs/languages/python/pymongo-driver/current/crud/transactions/
"""

import argparse
import hashlib
import json
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from pymongo import MongoClient, ReplaceOne, WriteConcern
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.spark_processing import build_documents, parse_kafka_records
from jobs.es_http import bulk_index_partition


def clean_records(raw, boundary):
    kafka = raw.selectExpr(
        "raw_value AS value", "topic", "partition", "offset", "kafka_timestamp AS timestamp"
    )
    parsed = parse_kafka_records(kafka, allow_backfill=True)
    valid = parsed.filter(F.col("error").isNull() & (F.col("event_time") < F.lit(boundary)))
    return build_documents(valid).withColumn("event_date", F.date_format("event_time", "yyyy-MM-dd"))


def daily_metrics(clean):
    return clean.groupBy("event_date", "source", "site").agg(
        F.count("*").alias("total"),
        *[
            F.sum(F.when(F.col("sentiment.label") == label, 1).otherwise(0)).alias(label)
            for label in ("positive", "negative", "neutral")
        ],
        F.sum("sentiment.score").alias("score_sum"),
    )


def mongo_client(uri, password_file):
    return MongoClient(
        uri,
        username="vietpulse",
        password=Path(password_file).read_text(),
        authSource="admin",
        serverSelectionTimeoutMS=10000,
    )


def commit_daily(client, documents, boundary, run_id):
    database = client["vietpulse"]
    # No serving writes take place until all Spark/Elasticsearch actions finish.
    with client.start_session() as session:
        with session.start_transaction(write_concern=WriteConcern(w="majority", j=True)):
            previous = database.meta.find_one({"_id": "batch"}, session=session)
            if previous and previous["T"].replace(tzinfo=UTC) > boundary:
                raise ValueError("A publish cannot move meta.T backwards")
            database.batch_daily.delete_many(
                {"event_date": {"$lt": boundary.date().isoformat()}}, session=session
            )
            for start in range(0, len(documents), 100):
                operations = []
                for original in documents[start : start + 100]:
                    document = original.copy()
                    document["_id"] = json.dumps(
                        [document["event_date"], document["source"], document.get("site")]
                    )
                    operations.append(ReplaceOne({"_id": document["_id"]}, document, upsert=True))
                database.batch_daily.bulk_write(operations, ordered=False, session=session)
            database.meta.replace_one(
                {"_id": "batch"},
                {"_id": "batch", "T": boundary, "run_id": run_id, "published_at": datetime.now(UTC)},
                upsert=True,
                session=session,
            )
    # This cleanup follows the successful transaction; API reads still filter at T.
    database.rt_window.delete_many({"window_end": {"$lte": boundary}})


def register_table(spark, name, path, root):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS vietpulse LOCATION '{root}/curated/.catalog'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS vietpulse.{name} USING PARQUET LOCATION '{path}'")
    spark.sql(f"MSCK REPAIR TABLE vietpulse.{name} SYNC PARTITIONS")


def reference_candidates(clean):
    return (
        clean.select(F.explode(F.split(F.lower("normalized_text"), r"\W+")).alias("term"))
        .filter(F.length("term").between(2, 64))
        .groupBy("term")
        .count()
        .filter(F.col("count") >= 2)
    )


def reference_digest(rows):
    encoded = "\n".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True) for row in sorted(rows, key=lambda x: x["term"])
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("clean", "analytics", "publish", "reference-refresh"))
    parser.add_argument("--root", default="hdfs://hdfs-namenode.storage.svc.cluster.local:8020/vietpulse")
    parser.add_argument(
        "--through-date", type=date.fromisoformat, default=datetime.now(UTC).date() - timedelta(days=1)
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--register-catalog", action="store_true")
    parser.add_argument("--es-url", default="https://vietpulse-es-es-http.serving.svc:9200")
    parser.add_argument(
        "--mongo-uri", default="mongodb://vietpulse-mongodb-svc.serving.svc:27017/?directConnection=true"
    )
    parser.add_argument("--mongo-password-file", default="/var/run/vietpulse/mongo/password")
    parser.add_argument("--reference-action", choices=("prepare", "apply"), default="prepare")
    parser.add_argument("--approved-digest")
    parser.add_argument("--approved-by")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9._:/-]+", args.root) or not re.fullmatch(r"[a-z0-9-]{1,50}", args.run_id):
        parser.error("root must be a simple filesystem URI and run-id a short lowercase identifier")
    if args.through_date >= datetime.now(UTC).date():
        parser.error("Only complete UTC dates can be published")
    boundary = datetime.combine(args.through_date + timedelta(days=1), datetime.min.time(), UTC)
    builder = SparkSession.builder.appName(f"vietpulse-batch-{args.stage}")
    spark = builder.enableHiveSupport().getOrCreate() if args.register_catalog else builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    root = args.root.rstrip("/")
    try:
        if args.stage == "clean":
            records = clean_records(spark.read.parquet(f"{root}/raw"), boundary)
            if records.isEmpty():
                raise ValueError("No valid raw records for the requested complete dates")
            records.write.mode("overwrite").partitionBy("event_date").parquet(f"{root}/clean")
            if args.register_catalog:
                register_table(spark, "clean_documents", f"{root}/clean", root)
        elif args.stage == "analytics":
            clean = spark.read.parquet(f"{root}/clean").filter(F.col("event_time") < F.lit(boundary))
            daily_metrics(clean).write.mode("overwrite").partitionBy("event_date").parquet(
                f"{root}/curated/daily"
            )
            if args.register_catalog:
                register_table(spark, "daily_sentiment", f"{root}/curated/daily", root)
        elif args.stage == "publish":
            daily = spark.read.parquet(f"{root}/curated/daily").filter(
                F.col("event_date") < F.lit(boundary.date())
            )
            rows = daily.limit(100001).collect()
            if not rows or len(rows) > 100000:
                raise ValueError("Publish requires 1..100000 daily cells within the demo transaction budget")
            documents = [row.asDict(recursive=True) for row in rows]
            for document in documents:
                document["event_date"] = str(document["event_date"])
            clean = spark.read.parquet(f"{root}/clean").filter(F.col("event_time") < F.lit(boundary))
            clean.drop("event_date").foreachPartition(
                lambda records: bulk_index_partition(records, args.es_url, "vp-docs")
            )
            with mongo_client(args.mongo_uri, args.mongo_password_file) as client:
                commit_daily(client, documents, boundary, args.run_id)
        else:
            path = f"{root}/reference/candidates/{args.run_id}"
            if args.reference_action == "prepare":
                candidates = reference_candidates(spark.read.parquet(f"{root}/clean"))
                candidates.write.mode("errorifexists").parquet(path)
            candidates = spark.read.parquet(path)
            rows = [row.asDict() for row in candidates.limit(100001).collect()]
            if len(rows) > 100000:
                raise ValueError("Reference candidates exceed the demo review budget")
            digest = reference_digest(rows)
            if args.reference_action == "apply":
                if args.approved_digest != digest or not args.approved_by:
                    raise ValueError(
                        "Reference apply requires an approver and the exact reviewed candidate digest"
                    )
                candidates.write.mode("overwrite").parquet(f"{root}/reference/approved-terms")
            print(
                json.dumps(
                    {
                        "reference_run": args.run_id,
                        "sha256": digest,
                        "terms": len(rows),
                        "action": args.reference_action,
                    }
                )
            )
        print(
            json.dumps(
                {
                    "stage": args.stage,
                    "run_id": args.run_id,
                    "through_date": str(args.through_date),
                    "ok": True,
                }
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
