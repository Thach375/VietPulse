"""Real Kafka → Spark → Parquet/MongoDB/Elasticsearch checks on loopback.

Invoked by serving_local_smoke.py so it shares the real authenticated MongoDB
replica set and HTTPS Elasticsearch. Local Parquet replaces HDFS in this test;
Kubernetes/PVC and Spark-to-HDFS/Hive checks still need the physical cluster.
"""

import json
import os
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from pymongo.errors import InvalidDocument
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from common.envelope import make_envelope  # noqa: E402
from crawlers.publish import fetch_records, publish_records  # noqa: E402
from jobs import batch, raw_ingest, speed_sentiment, speed_window  # noqa: E402
from jobs.es_http import request_json  # noqa: E402
from jobs.setup_elasticsearch import setup_elasticsearch  # noqa: E402


def run_processing_checks(work, service_env, mongo_uri, kafka_home, real_crawler=False):
    selected = {
        "VP_ES_URL": service_env["VP_ES_URL"],
        "VP_ES_USERNAME": "vp_probe",
        "VP_ES_PASSWORD_FILE": service_env["VP_ES_PASSWORD_FILE"],
        "VP_ES_CA_FILE": service_env["VP_ES_CA"],
        "PYSPARK_PYTHON": sys.executable,
    }
    previous = {key: os.environ.get(key) for key in selected}
    os.environ.update(selected)
    directory = work / "processing"
    directory.mkdir()
    listeners = [socket.socket(), socket.socket()]
    for listener in listeners:
        listener.bind(("127.0.0.1", 0))
    port, controller = [listener.getsockname()[1] for listener in listeners]
    for listener in listeners:
        listener.close()
    bootstrap = f"127.0.0.1:{port}"
    settings = {
        "process.roles": "broker,controller",
        "node.id": 0,
        "controller.quorum.bootstrap.servers": f"127.0.0.1:{controller}",
        "controller.listener.names": "CONTROLLER",
        "listeners": f"PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controller}",
        "advertised.listeners": f"PLAINTEXT://127.0.0.1:{port}",
        "listener.security.protocol.map": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "log.dirs": directory / "kafka-data",
        "offsets.topic.replication.factor": 1,
        "transaction.state.log.replication.factor": 1,
        "transaction.state.log.min.isr": 1,
        "num.network.threads": 2,
        "num.io.threads": 2,
        "group.initial.rebalance.delay.ms": 0,
    }
    config = directory / "kafka.properties"
    config.write_text("".join(f"{key}={value}\n" for key, value in settings.items()))
    kafka_env = os.environ | {
        "KAFKA_HEAP_OPTS": "-Xms128m -Xmx384m",
        "KAFKA_JVM_PERFORMANCE_OPTS": "-XX:ActiveProcessorCount=2",
    }
    cluster_id = subprocess.check_output(
        [str(kafka_home / "bin/kafka-storage.sh"), "random-uuid"], env=kafka_env, text=True
    ).strip()
    subprocess.run(
        [
            str(kafka_home / "bin/kafka-storage.sh"),
            "format",
            "--standalone",
            "-t",
            cluster_id,
            "-c",
            str(config),
        ],
        env=kafka_env,
        check=True,
        stdout=subprocess.DEVNULL,
        timeout=60,
    )
    log = (directory / "kafka.log").open("a")
    process = subprocess.Popen(
        [str(kafka_home / "bin/kafka-server-start.sh"), str(config)],
        env=kafka_env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    spark = None
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap})
        deadline = time.monotonic() + 120
        while True:
            try:
                if admin.list_topics(timeout=3).brokers:
                    break
            except Exception:
                if process.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError(f"Native Kafka failed; inspect {directory}") from None
                time.sleep(2)
        topics = ["raw.news.articles", "raw.news.comments", "raw.forum.posts", "raw.backfill", "dlq.raw"]
        for future in admin.create_topics(
            [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics]
        ).values():
            future.result(timeout=30)
        producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
        real_count = 0
        if real_crawler:
            receipts = publish_records(
                fetch_records("news-articles", ["vnexpress"], 1, 1), producer, directory / "crawler.sqlite", 1
            )
            if not receipts:
                raise ValueError("Real crawler fetched no record")
            receipt = receipts[0]
            consumer = Consumer(
                {"bootstrap.servers": bootstrap, "group.id": "vp-local-check", "enable.auto.commit": False}
            )
            try:
                consumer.assign([TopicPartition(receipt["topic"], receipt["partition"], receipt["offset"])])
                message = consumer.poll(30)
                assert message and not message.error() and json.loads(message.value()) == receipt["envelope"]
            finally:
                consumer.close()
            real_count = 1
            print("PASS: a real news crawler envelope matched its acknowledged Kafka offset.", flush=True)
        event = datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0) - timedelta(days=2)
        comment = make_envelope(
            "news_comment", "native-one", event, {"content": "rất tốt", "site": "vnexpress"}
        )
        forum = make_envelope("voz_post", "native-two", event, {"content": "rất tệ", "site": "voz"})
        for value, topic in (
            (comment, "raw.news.comments"),
            (comment, "raw.news.comments"),
            (forum, "raw.forum.posts"),
        ):
            producer.produce(topic, value=json.dumps(value))
        producer.produce("raw.news.comments", value="{broken")
        assert producer.flush(30) == 0
        ivy = directory / "ivy-settings.xml"
        ivy.write_text(
            '<ivysettings><settings defaultResolver="central"/><resolvers>'
            '<ibiblio name="central" m2compatible="true" root="https://repo.maven.apache.org/maven2"/>'
            "</resolvers></ivysettings>"
        )
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("native-processing")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4")
            .config("spark.jars.ivySettings", str(ivy))
            .config("spark.jars.ivy", str(directory / "ivy"))
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        root = directory / "dataset"
        raw_config = raw_ingest.JobConfig(
            bootstrap_servers=bootstrap,
            output_path=str(root / "raw"),
            checkpoint_location=str(directory / "raw-checkpoint"),
            available_now=True,
        )
        docs_config = speed_sentiment.JobConfig(
            bootstrap_servers=bootstrap,
            es_url=selected["VP_ES_URL"],
            checkpoint_location=str(directory / "docs-checkpoint"),
            available_now=True,
        )
        window_config = speed_window.WindowConfig(
            output_path=str(directory / "dedup"),
            dedup_checkpoint=str(directory / "dedup-checkpoint"),
            window_checkpoint=str(directory / "window-checkpoint"),
            mongo_uri=mongo_uri,
            mongo_password_file=str(work / "mongo-password"),
            available_now=True,
        )
        setup_elasticsearch(docs_config.es_url)

        def drain():
            for query in (
                raw_ingest.start_query(spark, raw_config),
                speed_sentiment.start_query(spark, docs_config),
            ):
                assert query.awaitTermination(180)
            assert speed_window.start_dedup_query(
                speed_sentiment.read_records(spark, docs_config), window_config
            ).awaitTermination(180)
            assert speed_window.start_window_query(spark, window_config).awaitTermination(180)

        drain()
        with batch.mongo_client(mongo_uri, work / "mongo-password") as client:
            assert sum(row["total"] for row in client.vietpulse.rt_window.find()) == 2 + real_count
        assert spark.read.parquet(str(root / "raw")).count() == 4 + real_count
        request_json(docs_config.es_url, "/_refresh", method="POST")
        assert request_json(docs_config.es_url, "/vp-docs/_count")["count"] == 2 + real_count
        assert request_json(docs_config.es_url, "/vp-dlq/_count")["count"] == 1
        producer.produce("raw.news.comments", value=json.dumps(comment))
        assert producer.flush(30) == 0
        drain()
        assert spark.read.parquet(str(root / "raw")).count() == 5 + real_count
        with batch.mongo_client(mongo_uri, work / "mongo-password") as client:
            assert sum(row["total"] for row in client.vietpulse.rt_window.find()) == 2 + real_count
        print(
            "PASS: Kafka → raw Parquet, ES documents/DLQ and MongoDB windows; "
            "checkpoints survived query restarts without double counting.",
            flush=True,
        )
        spark.stop()
        spark = None
        through_date = (datetime.now(UTC).date() - timedelta(days=1)).isoformat()
        common = [
            "--root",
            str(root),
            "--through-date",
            through_date,
            "--run-id",
            "native-check",
            "--es-url",
            docs_config.es_url,
            "--mongo-uri",
            mongo_uri,
            "--mongo-password-file",
            str(work / "mongo-password"),
        ]
        for stage in ("clean", "analytics", "publish"):
            SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", "2").getOrCreate()
            batch.main([stage, *common])
        with batch.mongo_client(mongo_uri, work / "mongo-password") as client:
            before = client.vietpulse.meta.find_one({"_id": "batch"})
            daily_before = list(client.vietpulse.batch_daily.find())
            assert before and len(daily_before) >= 2
            try:
                batch.commit_daily(
                    client,
                    [{"event_date": "2026-01-01", "source": "x", "bad": object()}],
                    datetime.now(UTC) + timedelta(days=1),
                    "must-not-commit",
                )
            except InvalidDocument:
                pass
            else:
                raise AssertionError("Invalid publish unexpectedly committed")
            assert client.vietpulse.meta.find_one({"_id": "batch"}) == before
            assert list(client.vietpulse.batch_daily.find()) == daily_before
        print(
            "PASS: clean → analytics → publish succeeded; "
            "a real MongoDB transaction failure preserved daily views and meta.T.",
            flush=True,
        )
        print(
            "Local processing smoke PASSED; K3s placement/PVC and HDFS/Hive integration remain pending.",
            flush=True,
        )
    finally:
        if spark:
            spark.stop()
        from serving_local_smoke import stop_process

        stop_process(process)
        log.close()
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
