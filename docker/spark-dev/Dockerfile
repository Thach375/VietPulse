# Spark cho môi trường dev: chạy local mode, Python 3.11 như phần còn lại của repo.
# Keep Python 3.11 / Java 17 explicit, matching the Kubernetes application image.
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==4.0.4 pymongo==4.19.0

COPY spark-defaults.conf /opt/spark-conf/spark-defaults.conf
ENV SPARK_CONF_DIR=/opt/spark-conf \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH=/workspace

WORKDIR /workspace
CMD ["sleep", "infinity"]
