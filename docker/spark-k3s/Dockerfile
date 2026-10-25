# The official image supplies Spark, Kubernetes entrypoint and Scala 2.13 jars.
# Python 3.11 and Java 17 are explicit in the final runtime.
FROM apache/spark:4.0.4-scala2.13-java17-python3-ubuntu AS spark
FROM maven:3.9.9-eclipse-temurin-17 AS jars
COPY docker/spark-k3s/dependencies.xml /build/pom.xml
COPY docker/spark-k3s/maven-settings.xml /build/settings.xml
RUN mvn -B -s /build/settings.xml -f /build/pom.xml -Pkafka org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies -DincludeScope=runtime -DincludeArtifactIds=spark-sql-kafka-0-10_2.13,spark-token-provider-kafka-0-10_2.13,kafka-clients,commons-pool2 -DoutputDirectory=/jars/kafka \
 && mvn -B -s /build/settings.xml -f /build/pom.xml -Phive org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies -DincludeScope=runtime -DoutputDirectory=/jars/hive

FROM python:3.11-slim-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless bash tini gosu procps libnss-wrapper \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --uid 185 --gid 0 --create-home spark
COPY --from=spark /opt/spark /opt/spark
COPY --from=spark /opt/entrypoint.sh /opt/entrypoint.sh
COPY --from=spark /opt/decom.sh /opt/decom.sh
# Keep Spark's existing Scala/SLF4J/compression versions; copy only missing connectors.
COPY --from=jars /jars/kafka/ /opt/spark/jars/
COPY --from=jars /jars/hive/ /opt/hive-client/
WORKDIR /opt/vietpulse
COPY pyproject.toml ./
COPY common/ common/
COPY jobs/ jobs/
COPY crawlers/ crawlers/
COPY infra/scripts/processing_control.py infra/scripts/processing_control.py
COPY docker/spark-k3s/check_runtime.py docker/spark-k3s/check_runtime.py
RUN pip install --no-cache-dir '.[processing]' \
 && mkdir -p /opt/spark/work-dir /tmp/spark-local \
 && chown -R 185:0 /opt/spark/work-dir /tmp/spark-local
ENV SPARK_HOME=/opt/spark \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH=/opt/vietpulse:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    SPARK_LOCAL_DIRS=/tmp/spark-local
ENV PATH=/opt/spark/bin:$PATH
USER 185
RUN spark-submit --master 'local[1]' docker/spark-k3s/check_runtime.py
WORKDIR /opt/spark/work-dir
ENTRYPOINT ["/opt/entrypoint.sh"]
