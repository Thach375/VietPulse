#!/usr/bin/env bash
set +xv
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

foundation_args=()
smoke=false
check_pipelines=false
check_ingestion=false
while (($#)); do
    case "$1" in
        --smoke) smoke=true; shift ;;
        --check-pipelines) check_pipelines=true; shift ;;
        --check-ingestion) check_ingestion=true; shift ;;
        --env-file)
            (($# >= 2)) || fail '--env-file requires a file'
            foundation_args+=("$1" "$2")
            shift 2 ;;
        --dry-run) foundation_args+=("$1"); shift ;;
        --help|-h)
            printf 'Usage: bash %s [--env-file FILE] [--dry-run] [--smoke] [--check-pipelines] [--check-ingestion]\n' "${0##*/}"
            exit 0 ;;
        *) fail 'Unknown argument; use --help' ;;
    esac
done
if ! $smoke && ! $check_pipelines && ! $check_ingestion; then
    smoke=true
    check_pipelines=true
    check_ingestion=true
fi
foundation_init "${foundation_args[@]}"
spark_version=$(version_pin processing spark)
operator_version=$(version_pin processing operator_version)
smoke_image=$(version_pin processing smoke_image)
if $dry_run; then
    printf 'DRY RUN: verify Spark Operator %s, Spark %s, Node 1 operator placement and driver RBAC.\n' "$operator_version" "$spark_version"
    $smoke && printf 'DRY RUN: submit a temporary SparkPi application on Nodes 3/4 and require completion.\n'
    $check_pipelines && printf 'DRY RUN: validate deployed streaming/batch jobs, checkpoints and sequential batch orchestration.\n'
    $check_ingestion && printf 'DRY RUN: require published crawler artifacts and a real valid Kafka envelope.\n'
    exit 0
fi
cluster_gate
if $smoke; then
    on_node 1 "$smoke_image" "$spark_version" <<'REMOTE'
image=$1
version=$2
for workload in deployment/spark-operator-controller deployment/spark-operator-webhook; do
    k -n processing rollout status "$workload" --timeout=300s
done
k -n processing get serviceaccount/spark role/spark rolebinding/spark >/dev/null
app="vp-spark-smoke-$(cat /proc/sys/kernel/random/uuid)"
k -n processing create -f - <<YAML
# SparkApplication fields and node selectors follow the pinned operator CRD.
# https://kubeflow.github.io/spark-operator/docs/api-docs.html
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: $app
  labels: {vietpulse.io/probe: processing}
spec:
  type: Scala
  mode: cluster
  image: $image
  imagePullPolicy: IfNotPresent
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples.jar
  sparkVersion: "$version"
  restartPolicy: {type: Never}
  driver:
    cores: 1
    memory: 512m
    serviceAccount: spark
    nodeSelector: {vietpulse.io/streaming: "true"}
  executor:
    instances: 1
    cores: 1
    memory: 512m
    nodeSelector: {vietpulse.io/data: "true"}
YAML
deadline=$((SECONDS + 600))
while ((SECONDS < deadline)); do
    state=$(k -n processing get "sparkapplication/$app" -o jsonpath='{.status.applicationState.state}')
    case "$state" in
        COMPLETED) break ;;
        FAILED|SUBMISSION_FAILED|FAILING)
            fail "Spark operator smoke application reached $state; inspect sparkapplication/$app" ;;
    esac
    sleep 5
done
[[ $state == COMPLETED ]] || fail "Spark operator smoke timed out; inspect sparkapplication/$app"
driver=$(k -n processing get "sparkapplication/$app" -o jsonpath='{.status.driverInfo.podName}')
[[ -n $driver ]] || fail 'Completed Spark application has no driver pod reference'
driver_node=$(k -n processing get "pod/$driver" -o jsonpath='{.spec.nodeName}')
[[ $driver_node == vp-node-3 ]] || fail "SparkPi driver ran on $driver_node rather than Node 3"
k -n processing logs "pod/$driver" | grep -q 'Pi is roughly' || fail 'SparkPi output missing from driver logs'
k -n processing delete "sparkapplication/$app" --wait=true --timeout=90s >/dev/null
printf 'PASS: Spark Operator completed SparkPi with driver on Node 3 and executor selector for Node 4.\n'
REMOTE
fi
if $check_pipelines || $check_ingestion; then
    fail 'Pipeline and ingestion checks require published application images and are not configured yet'
fi
