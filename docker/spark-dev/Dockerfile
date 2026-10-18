# Spark cho môi trường dev: chạy local mode, Python 3.11 như phần còn lại của repo.
# Image Spark chính thức đi kèm Python 3.10, không chạy được code trong common/.
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==3.5.3

COPY spark-defaults.conf /opt/spark-conf/spark-defaults.conf
ENV SPARK_CONF_DIR=/opt/spark-conf \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH=/workspace

WORKDIR /workspace
CMD ["sleep", "infinity"]
