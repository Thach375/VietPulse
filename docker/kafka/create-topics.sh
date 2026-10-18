#!/bin/sh
# Tạo Kafka topic theo hợp đồng dữ liệu (mục 3 của kế hoạch). Chạy lại nhiều lần vẫn an toàn.
set -eu

BOOTSTRAP=kafka:9092
HOUR=3600000
DAY=86400000

create() {
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1 --config retention.ms="$3"
}

create raw.news.articles 3 $((7 * DAY))
create raw.news.comments 6 $((7 * DAY))
create raw.forum.posts   3 $((7 * DAY))
create raw.backfill      6 $((2 * DAY))
create dlq.raw           1 $((14 * DAY))
create alerts.sentiment  1 $((7 * DAY))
# Chỉ có ở môi trường dev, dùng cho docker/spark-dev/smoke_test.py.
create dev.smoke         1 $((1 * HOUR))

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
