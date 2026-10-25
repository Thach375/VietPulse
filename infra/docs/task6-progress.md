# Task 6 implementation handoff

## Working branch and user constraints

Continue all K3s deployment work on **`k3s`**. The user requested that Tasks
3, 4, 5 and 6 share that branch; do not create another branch per task.
Task 6 is **in progress**, not accepted as complete.

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
  the exact chart CRD schema. The live command has not run. The pipeline and
  ingestion check flags currently fail explicitly because those checks are
  not implemented yet; dry runs make no SSH connections.
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
- The latest increment wires the optional window queries into
  `jobs/speed_sentiment.py` and adds their directories to the HDFS bootstrap
  and probe. Full Kafka/HDFS/MongoDB/Elasticsearch integration remains untested.

## Outstanding work

1. Align project dependencies and images with Spark 4.0.4 / Scala 2.13 / Java 17.
   `pyproject.toml` and the Compose development image still pin Spark 3.5.3.
   PyMongo 4.19.0 is installed only in the temporary test environment and must
   be declared for the application; importing the new window module requires
   it. Existing development instructions also need updating for this migration.
2. Verify the new MongoDB window sink and Elasticsearch HTTPS client against
   actual services, including retries/restarts. Check query startup ordering,
   checkpoint isolation and failure propagation through the complete job.
3. Implement Kafka-producing crawler entrypoints and durable deduplication,
   reusing the existing news/VOZ fetchers and envelope/topic contracts. First
   prove a real source produces a valid envelope, then enable all three sources.
4. Implement `batch-clean`, `batch-analytics`, `batch-publish` and
   `reference-refresh` against the existing project contracts. Publish must
   advance `meta.T` only after all required writes succeed.
5. Add application Dockerfiles and consume real published image references.
   No registry/tag/digest has been supplied; do not invent a published image.
6. Add processing/ingestion manifests and their Kustomizations, including
   placement, resources, Secret/CA mounts, HDFS/catalog configuration, separate
   checkpoints and one executor per application initially.
7. Add sequential clean → analytics → publish orchestration, stop on failure,
   prevent overlapping heavy runs and require explicit approval before
   reference-refresh is applied.
8. Finish `--check-pipelines` and `--check-ingestion`, failure-case tests and
   the Task 6 runbook. Full Task 6 acceptance must remain pending until actual
   images and the four-node cluster are available.

## Verified locally

The pinned Helm chart rendered successfully. SparkApplication smoke YAML was
validated against its CRD. Bash syntax, ShellCheck and Ruff passed for the
completed increments.

The latest targeted run passed **28 tests**:

```bash
/tmp/vietpulse-task2-venv/bin/pytest \
  tests/test_speed_sentiment.py tests/test_speed_window.py \
  tests/test_storage_deployment.py -q
```

Earlier Task 6 runs also passed the raw-ingest checkpoint/file-sink tests and
the Elasticsearch bulk/authentication tests. A full repository run is still
required after the remaining Task 6 changes.

## Local tools and references

- Python environment: `/tmp/vietpulse-task2-venv` (Python 3.12, PySpark 4.0.4,
  PyMongo 4.19.0, pytest, PyYAML, Ruff and ShellCheck).
- Helm: `/tmp/linux-amd64/helm`; kubectl: `/tmp/vietpulse-kubectl`.
- Spark Operator chart: `/tmp/vietpulse-spark-chart/spark-operator` (2.5.2).
- Native serving distributions: `/tmp/vietpulse-serving-tools`.
- [Spark 4.0.4 stateful operation restrictions](https://spark.apache.org/docs/4.0.4/streaming/apis-on-dataframes-and-datasets.html#unsupported-operations):
  deduplication followed by aggregation cannot be chained in Update mode; this
  is the reason the aggregation branch uses two queries.
- [Spark 4.0.4 Kafka integration](https://spark.apache.org/docs/4.0.4/streaming/structured-streaming-kafka-integration.html).
- [Spark Operator API](https://kubeflow.github.io/spark-operator/docs/api-docs.html);
  use the pinned chart CRD to validate fields because the site includes older
  examples.
- [Elasticsearch 8.19 Bulk API](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/docs-bulk.html).
