# Task 6 implementation handoff

## Working branch and user constraints

Continue all K3s deployment work on **`k3s`**. The user requested that Tasks
3, 4, 5 and 6 share that branch; do not create another branch per task.
Task 6 application/deployment code is implemented. **Live acceptance remains
pending image publication and the four-node cluster.**

Only Node 1 is available. Keep the four-node placement and production demo
resource budgets in deployment code, and verify locally where possible.
There is no live four-node K3s acceptance result. Docker access was unavailable;
native distributions and local Spark tests have been used instead.

`TASKS.md` and `VietPulse-K3s-Deployment-PRD.md` are existing untracked input
documents. Read them, but do not automatically add them to a commit.

## Implemented increments

- Task 3 Kafka infrastructure and Tasks 4-5 storage/serving infrastructure are
  included in the branch history. Task 4 and Task 5 were saved together in
  commit `2f6d121`.
- `k8s/base/processing/operator-values.yaml` configures Spark Operator 2.5.2,
  limits watched jobs to `processing`, creates the `spark` driver service
  account/RBAC, and places controller/webhook on Node 1.
- `infra/scripts/verify-processing.sh --smoke` submits a temporary SparkPi
  application with the pinned official Spark 4.0.4 image. Its template passed
  the exact chart CRD schema. The live command has not run. Pipeline/ingestion
  flags validate runtime resources, a real crawler/Kafka receipt and an actual
  sequential batch chain; missing published artifacts fail explicitly.
  Dry runs make no SSH connections.
- `jobs/raw_ingest.py` preserves raw Kafka values and coordinates in a Parquet
  file sink with its own checkpoint. Local Spark tests cover malformed values,
  checkpoint reuse and additional input without duplicate committed output.
- `jobs/speed_window.py` separates deduplication and window aggregation into
  two streaming queries, using an intermediate Parquet directory and separate
  checkpoints. The MongoDB sink replaces absolute counts using stable keys,
  majority write concern and journaling.
- `jobs/es_http.py` supports authenticated HTTPS with a mounted CA/password
  and stable-ID bulk indexing. Per-item failures raise an error. Query A and
  `jobs/setup_elasticsearch.py` now use this client instead of the Elasticsearch
  Hadoop connector.
- The streaming entrypoint wires the optional window queries into
  `jobs/speed_sentiment.py` and adds their directories to the HDFS bootstrap
  and probe. The native pipeline and separate Spark/HDFS/Hive checks now pass.
- Kafka-producing crawler entrypoints, durable acknowledgement state, all four
  batch stages and the shared heavy-run controller are implemented. MongoDB
  daily views, real-time cleanup and `meta.T` commit in one transaction.
- The processing/ingestion bases, immutable image renderer, Secret-copy helper,
  application Dockerfiles, GHCR build workflow and live verifier are complete.
  See [the Task 6 runbook](processing-runbook.md) for exact commands and gates.

## Outstanding work

1. Run the complete Docker builds in CI or on a machine with Docker access.
   Native Maven resolution and Spark/Hive client checks have passed; the local
   Docker socket still denies access. The workflow builds on `k3s` pushes and
   publishes only on explicit manual input or an `images-*` release tag.
2. Publish the two images and fill `infra/local/application-images.yaml` with
   their actual digests. No published digest has been invented or recorded.
3. When Nodes 2-4 are available, complete the earlier live tier gates, render
   the release, provision namespace-local Secret copies, and run the full
   Task 6 verifier against actual operators, pod placement and PVCs.
4. Enable scheduled ingestion/batch only after those gates pass. Complete the
   broader physical failure/replay acceptance suite in Task 8.

## Verified locally

The pinned Helm chart rendered successfully. SparkApplication smoke YAML was
validated against its CRD. Bash syntax, ShellCheck and Ruff passed for the
completed increments.

The final full repository test run passed **208 tests**, with Spark tests
enabled and no skips. Relevant tests can be run with:

```bash
/tmp/vietpulse-task2-venv/bin/pytest -q
```

The native service test passed a real crawler/Kafka offset check, streaming
checkpoint restarts, Elasticsearch document/DLQ writes, MongoDB windows and
all three daily stages. A real failed MongoDB transaction preserved daily
views and `meta.T`. The separate catalog test verified Spark 4.0.4 writing
replicated Parquet to HDFS and reading partitions through Hive Metastore 4.0.1.

## Local tools and references

- Python environment: `/tmp/vietpulse-task2-venv` (Python 3.12, PySpark 4.0.4,
  PyMongo 4.19.0, pytest, PyYAML, Ruff and ShellCheck).
- Helm: `/tmp/linux-amd64/helm`; kubectl: `/tmp/vietpulse-kubectl`.
- Spark Operator chart: `/tmp/vietpulse-spark-chart/spark-operator` (2.5.2).
- Native serving distributions: `/tmp/vietpulse-serving-tools`.
- Resolved Kafka/Hive client jars: `/tmp/vietpulse-task6-jars`.
- Native pipeline evidence: `/tmp/vietpulse-task6-native.log`.
- Final native durability regression: `/tmp/vietpulse-task6-native-final.log`.
- Full test report: `/tmp/vietpulse-task6-pytest-final.log`.
- Native Spark/HDFS/Hive evidence: `/tmp/vietpulse-task6-catalog.log`.
- [Spark 4.0.4 stateful operation restrictions](https://spark.apache.org/docs/4.0.4/streaming/apis-on-dataframes-and-datasets.html#unsupported-operations):
  deduplication followed by aggregation cannot be chained in Update mode; this
  is the reason the aggregation branch uses two queries.
- [Spark 4.0.4 Kafka integration](https://spark.apache.org/docs/4.0.4/streaming/structured-streaming-kafka-integration.html).
- [Spark Operator API](https://kubeflow.github.io/spark-operator/docs/api-docs.html);
  use the pinned chart CRD to validate fields because the site includes older
  examples.
- [Elasticsearch 8.19 Bulk API](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/docs-bulk.html).
