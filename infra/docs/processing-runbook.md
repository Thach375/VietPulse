# Task 6: ingestion and processing

All deployment work stays on branch `k3s`. Application code and deployment
definitions are implemented. Native verification on Node 1 covers the real
pipeline services and, separately, Spark-to-HDFS/Hive integration. Four-node
placement, operator reconciliation and PVC acceptance remain pending until
Nodes 2-4 are available.

## Runtime and data contracts

Spark Operator 2.5.2 manages Spark 4.0.4 / Scala 2.13 / Java 17 / Python 3.11.
The application image packages the Kafka connector and isolated Hive 4.0.1
client jars. MongoDB/Elasticsearch writes use Python clients; the Elasticsearch
client verifies the mounted ECK certificate and rejects plaintext credentials.

| Application | Driver | One executor | Driver heap + overhead | Executor heap + overhead |
| --- | --- | --- | --- | --- |
| raw-ingest | Node 3 | Node 3 or 4 | 768 + 256Mi | 1024 + 512Mi |
| speed-sentiment | Node 3 | Node 3 or 4 | 1024 + 384Mi | 1536 + 768Mi |
| Each batch/reference stage | Node 2 | Node 2 or 4 | 1024 + 384Mi | 4096 + 1024Mi |

The operator/controller jobs select Node 1. Three crawlers select Node 3,
each with a 384Mi memory limit, and share one 2Gi local-path state claim.
Per-source SQLite files and process locks prevent overlapping local updates.
Crawler IDs enter SQLite only after an `acks=all` Kafka acknowledgement.
A crash between the Kafka and SQLite commits can replay a record; downstream
deduplication and stable serving IDs handle that at-least-once boundary.

`raw-ingest` preserves valid and malformed values plus Kafka coordinates in
Parquet under `/vietpulse/raw`, partitioned by topic and Kafka ingestion date.
The demo batch reads all retained raw records, accepts valid `raw.backfill`,
chooses the latest source/record ID, and rebuilds complete UTC-date partitions.
This includes late arrivals without assuming that only yesterday changed.
Clean and daily sentiment tables are registered in the Task 4 Hive catalog.

Speed documents are upserted into `vp-docs`; invalid records go to Kafka
`dlq.raw` and the `vp-dlq` index alias. The existing rules model is used.
Business thresholds for `alerts.sentiment` remain a separate project contract;
this deployment does not invent an alert rule.

The aggregation branch deduplicates source/record IDs within a six-hour
watermark, writes intermediate Parquet and updates five-minute MongoDB cells
using absolute counts. Separate HDFS state paths are mandatory:

```text
/vietpulse/checkpoints/raw-ingest
/vietpulse/checkpoints/speed-sentiment-a
/vietpulse/checkpoints/speed-sentiment-b-dedup
/vietpulse/checkpoints/speed-sentiment-b
/vietpulse/checkpoints/speed-sentiment-dedup-output  (intermediate data)
```

The two aggregation queries avoid Spark's restriction on chaining stateful
deduplication and aggregation in Update mode. Preserve all five directories
across application restarts. Stateful schema changes require a checkpoint
migration; Compose checkpoints from Spark 3.5 are not Kubernetes checkpoints.

`batch-clean → batch-analytics → batch-publish` runs sequentially at 01:00
Asia/Ho_Chi_Minh. Data days and the exclusive boundary `T` use UTC. The demo
publish transaction supports up to 100,000 daily cells. After Elasticsearch
succeeds, one MongoDB transaction replaces `batch_daily`, removes obsolete
`rt_window` cells and writes `meta/{_id: "batch", T, run_id, published_at}`.
A failed transaction leaves those collections and T unchanged. Task 7 readers
must combine daily values before T with real-time windows at/after T.

## Build images

Docker is required only for local container builds. Coding and native tests can
run without it. Rebuild Compose's Spark image after this version migration.
The `Application images` GitHub workflow builds both images on relevant pushes
to `k3s`. Its manual `publish=true` input publishes to the repository's GHCR
namespace and prints the actual digest references. An explicitly pushed
`images-*` release tag also publishes, allowing release from `k3s` before the
workflow is present on the repository's default branch. The workflow uses
`GITHUB_TOKEN`, not a password committed to the repository. Publication has
not been performed by the implementation agent.

For local builds, from the repository root:

```bash
docker build -f docker/crawler/Dockerfile -t vietpulse-crawler:task6 .
docker build -f docker/spark-k3s/Dockerfile -t vietpulse-spark:task6 .
```

The Spark image build resolves pinned Kafka/Hive client dependencies from Maven
Central and runs a Spark/Kafka-classpath/Python runtime check. These builds need
outbound HTTPS. Keep GHCR packages public for the demo, or provision the same
image-pull Secret name separately in `processing` and `ingestion` and supply
`--pull-secret NAME` when rendering.

## Local verification on Node 1

Use a virtual environment with the development extras and Java 17:

```bash
python -m pip install -e '.[crawler,streaming,dev]'
python -m pytest -q
ruff check .
ruff format --check .
shellcheck -x -P SCRIPTDIR infra/scripts/*.sh infra/scripts/lib/*.sh
```

With the official, checksum-verified distributions from the Task 3/5 runbooks:

```bash
python tests/integration/serving_local_smoke.py \
  --mongod /absolute/path/to/mongodb/bin/mongod \
  --mongosh /absolute/path/to/mongosh/bin/mongosh \
  --elasticsearch-home /absolute/path/to/elasticsearch-8.19.23 \
  --kibana-home /absolute/path/to/kibana-8.19.23 \
  --processing-kafka-home /absolute/path/to/kafka_2.13-4.2.1 \
  --processing-real-crawler --processing-only
```

This starts isolated native services, fetches one real news article, verifies
its acknowledged Kafka offset, processes valid/duplicate/malformed records,
restarts streaming queries using their existing checkpoints, runs the three
batch stages and injects a transaction failure. It uses local Parquet instead
of HDFS. Native Kafka is reduced to one broker for this test; deployment
replication remains three. All listeners are loopback; random credentials and
data stay in the private printed temporary directory. Only its own processes
are stopped. Network/source rejection makes the real-crawler gate fail visibly.

The Task 4 native storage test can also verify the isolated Hive client jars
from the image build against two real HDFS DataNodes and Hive Metastore:

```bash
python tests/integration/storage_local_smoke.py \
  --java-home /absolute/path/to/java8 \
  --hadoop-home /absolute/path/to/hadoop-3.3.6 \
  --hive-home /absolute/path/to/apache-hive-4.0.1-bin \
  --postgres-bin /absolute/path/to/postgresql-16.15/bin \
  --jdbc-jar /absolute/path/to/postgresql-42.7.8.jar \
  --spark-client-jars /absolute/path/to/resolved-hive-client-jars --spark-only
```

The harness uses Java 8 for the existing Hadoop/Hive distributions and the
host's Java 17+ for Spark. It verifies a replicated Spark Parquet write and
partition reads through the real catalog. PostgreSQL's native shared libraries
must also be available, as described in Task 4.

## Deploy when Tasks 2-5 pass on four nodes

Install Helm and development Python dependencies on Node 1. Validate the
foundation, Kafka, HDFS/catalog and serving tiers first. Reapply the storage
base and run the HDFS bootstrap to add the new intermediate/checkpoint paths:

```bash
sudo k3s kubectl apply -k k8s/base/storage
sudo k3s kubectl -n storage exec hdfs-namenode-0 -- bash /etc/hadoop/conf/bootstrap-hdfs.sh
source infra/scripts/lib/common.sh
repo_root=$PWD
sudo helm upgrade --install spark-operator "$(version_pin processing helm_chart)" \
  --repo "$(version_pin processing helm_repository)" \
  --version "$(version_pin processing chart_version)" \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --namespace processing \
  -f k8s/base/processing/operator-values.yaml --wait --timeout 5m
sudo k3s kubectl wait --for=condition=Established \
  crd/sparkapplications.sparkoperator.k8s.io --timeout=120s
bash infra/scripts/configure-processing-secrets.sh
mkdir -p infra/local
cp infra/application-images.example.yaml infra/local/application-images.yaml
```

Fill the ignored local image file with the two **actual published** references
including `@sha256:...`. Render before applying; missing image inputs fail
before a cluster call. The source Kustomization alone keeps all applications
and schedules suspended.

```bash
python infra/scripts/render_processing.py --images-file infra/local/application-images.yaml \
  > infra/local/task6-release.yaml
sudo k3s kubectl apply -f infra/local/task6-release.yaml
bash infra/scripts/verify-processing.sh --smoke --check-ingestion --check-pipelines
```

The Secret helper copies the existing MongoDB password and ECK password/public
certificate into namespace-local Secrets without printing values. Repeat it
after deliberate credential/certificate rotation and restart dependent pods.
Keep initialization Secrets and all state claims on redeployment.

The verifier first requires a real Node 3 crawler receipt and consumes its exact
Kafka offset. It then checks operator and application image/version, actual
streaming pod placement/readiness, resources and separate HDFS paths. It starts
one real clean/analytics/publish chain with a 15-minute demo deadline and checks
the completed sequence and batch-driver placement. Supply valid historical raw
data from completed UTC dates before this batch check; an empty dataset is a
failed gate. A failed probe Job is retained for diagnosis. Completed batch
applications are retained for one day. `--dry-run` and `--env-file FILE` follow
the existing inventory/SSH convention and never contact hosts in dry-run mode.

After the gates pass, enable the crawlers and daily batch schedule:

```bash
sudo k3s kubectl -n ingestion patch cronjob news-articles --type=merge -p '{"spec":{"suspend":false}}'
sudo k3s kubectl -n ingestion patch cronjob news-comments --type=merge -p '{"spec":{"suspend":false}}'
sudo k3s kubectl -n ingestion patch cronjob voz --type=merge -p '{"spec":{"suspend":false}}'
sudo k3s kubectl -n processing patch cronjob vietpulse-batch --type=merge -p '{"spec":{"suspend":false}}'
```

Rendering/reapplying the release suspends schedules again for review. Re-enable
them after verification. To roll back an image, render the previous immutable
image file and apply it while preserving compatible checkpoints and claims.

## Reference approval and heavy-run recovery

The weekly reference schedule prepares candidate term frequencies only. It is
suspended initially. Create one preparation Job from its template:

```bash
sudo k3s kubectl -n processing create job reference-review --from=cronjob/reference-refresh-prepare
```

Read the completed child Spark application's driver logs for `reference_run`
and `sha256`, and review `/vietpulse/reference/candidates/<reference_run>`.
An authorized reviewer creates `processing/reference-refresh-approval` with
keys `run_id`, `sha256`, and `approved_by` matching the reviewed candidate.
The controller service account can read this one ConfigMap but cannot approve
or modify it. Create an apply Job from the same CronJob template, changing the
container args to `["reference-apply"]`. The application recomputes the exact
candidate digest before writing `/vietpulse/reference/approved-terms`.

For example, after substituting the reviewed values and reviewer identity:

```bash
sudo k3s kubectl -n processing create configmap reference-refresh-approval \
  --from-literal=run_id=REVIEWED_RUN --from-literal=sha256=REVIEWED_SHA256 \
  --from-literal=approved_by=REVIEWER --dry-run=client -o yaml | sudo k3s kubectl apply -f -
reference_job="reference-apply-$(date +%s)"
sudo k3s kubectl -n processing create job "$reference_job" \
  --from=cronjob/reference-refresh-prepare --dry-run=client -o json | \
  python3 -c 'import json,sys; job=json.load(sys.stdin); job["spec"]["template"]["spec"]["containers"][0]["args"]=["reference-apply"]; print(json.dumps(job))' | \
  sudo k3s kubectl -n processing create -f -
```

Daily and reference runs, including manually created Jobs, acquire the same
`processing/vietpulse-heavy-batch` Lease. The controller never steals a lock by
age. It releases it after children finish or cancellation is confirmed. A dead
controller/API failure leaves the lock for inspection. Check the owning Job,
Spark applications labelled `vietpulse.io/heavy=true`, and their driver pods
before removing an abandoned Lease. Never submit suspended heavy templates
directly or bypass the controller to start concurrent stages.

Useful diagnosis commands:

```bash
sudo k3s kubectl -n processing get sparkapplications,pods,jobs,cronjobs,leases
sudo k3s kubectl -n ingestion get jobs,pods,pvc
sudo k3s kubectl -n processing logs deployment/spark-operator-controller
sudo k3s kubectl -n processing get events --sort-by=.lastTimestamp
sudo k3s kubectl -n ingestion get events --sort-by=.lastTimestamp
```

Sources: [Spark Operator API](https://kubeflow.github.io/spark-operator/docs/api-docs.html),
[Spark 4.0.4 streaming restrictions](https://spark.apache.org/docs/4.0.4/streaming/apis-on-dataframes-and-datasets.html#unsupported-operations),
[Hive client configuration](https://spark.apache.org/docs/4.0.4/configuration.html),
[CronJob concurrency](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#concurrency-policy),
[MongoDB transactions](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/crud/transactions/),
and [GitHub container publishing](https://docs.github.com/en/actions/tutorials/publish-packages/publish-docker-images).
