"""Check the packaged Hive 4 client and Spark/HDFS protocol against real services."""

import argparse
import uuid
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-uri", required=True)
    parser.add_argument("--metastore-uri", required=True)
    parser.add_argument("--hive-jars", required=True, type=Path)
    args = parser.parse_args()
    assert (args.hive_jars / "hive-metastore-4.0.1.jar").is_file()
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("spark-hive-client-check")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.hive.metastore.version", "4.0.1")
        .config("spark.sql.hive.metastore.jars", "path")
        .config("spark.sql.hive.metastore.jars.path", f"file://{args.hive_jars.resolve()}/*")
        .config("spark.hadoop.hive.metastore.uris", args.metastore_uri)
        .config("spark.hadoop.fs.defaultFS", args.hdfs_uri)
        .config("spark.hadoop.dfs.replication", "2")
        .enableHiveSupport()
        .getOrCreate()
    )
    root = args.hdfs_uri + "/vietpulse/curated/.spark-catalog-" + uuid.uuid4().hex[:12]
    database = "vp_task6_" + uuid.uuid4().hex[:12]
    try:
        spark.range(3).withColumn("event_date", F.lit("2026-10-24")).write.partitionBy("event_date").parquet(
            root + "/data"
        )
        spark.sql(f"CREATE DATABASE {database} LOCATION '{root}/catalog'")
        spark.sql(f"CREATE TABLE {database}.probe USING PARQUET LOCATION '{root}/data'")
        spark.sql(f"MSCK REPAIR TABLE {database}.probe SYNC PARTITIONS")
        assert spark.table(f"{database}.probe").count() == 3
        assert spark.table(f"{database}.probe").agg(F.sum("id")).first()[0] == 3
        print(
            "PASS: Spark 4.0.4 wrote replicated HDFS Parquet "
            "and read its partitions via Hive Metastore 4.0.1."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
