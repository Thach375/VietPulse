"""Executed inside the image build; a jar/image mismatch fails the build."""

import sys
from pathlib import Path

import pymongo
from pyspark.sql import SparkSession

assert sys.version_info[:2] == (3, 11)
assert pymongo.version == "4.19.0"
assert Path("/opt/hive-client/hive-metastore-4.0.1.jar").is_file()
assert len(list(Path("/opt/spark/jars").glob("scala-library-*.jar"))) == 1
assert len(list(Path("/opt/spark/jars").glob("slf4j-api-*.jar"))) == 1
spark = SparkSession.builder.appName("image-check").getOrCreate()
try:
    assert spark.version == "4.0.4"
    spark._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass(
        "org.apache.spark.sql.kafka010.KafkaSourceProvider"
    )
    assert spark.range(4).count() == 4
finally:
    spark.stop()
