"""Kiểm môi trường dev (M1.4): Spark ghi và đọc lại được Kafka, MongoDB và Elasticsearch.

Chạy: docker compose exec spark spark-submit docker/spark-dev/smoke_test.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("vietpulse-smoke-test").getOrCreate()
df = spark.createDataFrame(
    [("smoke:1", "Xin chào VietPulse"), ("smoke:2", "Kiểm tra kết nối")], ["record_id", "content"]
)
results = {}

# Kafka: topic là log chỉ thêm vào cuối, nên số bản ghi tăng 2 sau mỗi lần chạy.
kafka = {"kafka.bootstrap.servers": "kafka:9092"}
df.selectExpr("record_id AS key", "content AS value").write.format("kafka").options(**kafka).option(
    "topic", "dev.smoke"
).save()
results["Kafka dev.smoke"] = (
    spark.read.format("kafka")
    .options(**kafka)
    .option("subscribe", "dev.smoke")
    .option("startingOffsets", "earliest")
    .load()
    .count()
)

mongo = {"connection.uri": "mongodb://mongodb:27017", "database": "vietpulse", "collection": "smoke_test"}
df.write.format("mongodb").mode("overwrite").options(**mongo).save()
results["MongoDB vietpulse.smoke_test"] = spark.read.format("mongodb").options(**mongo).load().count()

es = {"es.nodes": "elasticsearch", "es.port": "9200", "es.nodes.wan.only": "true"}
df.write.format("es").mode("append").options(**es).option("es.mapping.id", "record_id").save("vp-smoke-test")
results["Elasticsearch vp-smoke-test"] = spark.read.format("es").options(**es).load("vp-smoke-test").count()

for name, count in results.items():
    print(f"{name}: {count} bản ghi")
failed = [name for name, count in results.items() if count < 2]
spark.stop()
if failed:
    raise SystemExit(f"Thiếu bản ghi ở: {', '.join(failed)}")
print("Môi trường dev OK")
