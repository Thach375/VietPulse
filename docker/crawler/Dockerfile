FROM python:3.11-slim-bookworm
WORKDIR /opt/vietpulse
COPY pyproject.toml ./
COPY common/ common/
COPY crawlers/ crawlers/
COPY jobs/ jobs/
RUN pip install --no-cache-dir '.[crawler]' \
 && useradd --uid 1000 --create-home crawler \
 && mkdir /state && chown 1000:1000 /state \
 && python3 -c 'import confluent_kafka; from crawlers import publish; assert len(publish.TOPICS) == 3'
USER 1000:1000
ENTRYPOINT ["python3", "-m", "crawlers.publish"]
