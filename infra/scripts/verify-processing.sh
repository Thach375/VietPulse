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
    work_dir=$(mktemp -d)
    trap 'rm -rf -- "$work_dir"' EXIT
    on_node 1 <<'REMOTE'
k -n processing get configmap/vietpulse-image-pins >/dev/null || fail 'Published application artifacts missing; render with real image digests first'
REMOTE
fi
if $check_ingestion; then
    checks=$(base64 -w0 "$repo_root/infra/scripts/lib/processing_checks.py")
    envelope=$(base64 -w0 "$repo_root/common/envelope.py")
    on_node 1 "$checks" "$envelope" <<'REMOTE'
work_dir=$(mktemp -d)
mkdir "$work_dir/common"
printf '%s' "$1" | base64 -d > "$work_dir/checks.py"
printf '%s' "$2" | base64 -d > "$work_dir/common/envelope.py"
job="vp-crawler-check-$(cat /proc/sys/kernel/random/uuid)"
cleanup() {
    result=$?
    trap - EXIT
    if ((result == 0)); then
        k -n ingestion delete job "$job" --ignore-not-found --wait=true --timeout=60s >/dev/null || true
    else
        printf 'Crawler probe job %s retained for diagnosis.\n' "$job" >&2
    fi
    rm -rf -- "$work_dir"
    exit "$result"
}
trap cleanup EXIT
k -n ingestion get cronjob/news-articles -o json > "$work_dir/template.json"
python3 - "$work_dir/template.json" "$job" <<'PY' | k -n ingestion create -f -
import json,sys
cron=json.load(open(sys.argv[1]))
spec=cron['spec']['jobTemplate']['spec']
spec.update(activeDeadlineSeconds=600,backoffLimit=0)
spec['template']['spec']['containers'][0]['args']=[
 '--source','news-articles','--sites','vnexpress','--limit','1','--candidate-limit','10',
 '--state-dir','/state/probes/'+sys.argv[2],'--require-record','--print-receipt']
print(json.dumps({'apiVersion':'batch/v1','kind':'Job','metadata':{'name':sys.argv[2],
 'labels':{'vietpulse.io/probe':'ingestion'}},'spec':spec}))
PY
deadline=$((SECONDS + 660))
until [[ $(k -n ingestion get job "$job" -o jsonpath='{.status.succeeded}') == 1 ]]; do
    failed=$(k -n ingestion get job "$job" -o jsonpath='{.status.failed}')
    [[ ${failed:-0} == 0 ]] || fail 'Real crawler failed; inspect its probe Job logs'
    ((SECONDS < deadline)) || fail 'Crawler did not finish within eleven minutes'
    sleep 5
done
pod=$(k -n ingestion get pod -l "job-name=$job" -o jsonpath='{.items[0].metadata.name}')
[[ $(k -n ingestion get pod "$pod" -o jsonpath='{.spec.nodeName}') == vp-node-3 ]] || fail 'Crawler ran outside Node 3'
[[ $(k -n ingestion get pvc crawler-state -o jsonpath='{.status.phase}') == Bound ]] || fail 'Crawler state claim is not Bound'
k -n ingestion logs "pod/$pod" > "$work_dir/receipt.log"
read -r topic partition offset < <(PYTHONPATH="$work_dir" python3 "$work_dir/checks.py" receipt "$work_dir/receipt.log")
broker=$(k -n messaging get pods -l strimzi.io/cluster=vietpulse -o json | python3 -c '
import json,sys
pods=json.load(sys.stdin)["items"]
print(next(p["metadata"]["name"] for p in pods if any(c["name"]=="kafka" for c in p["spec"]["containers"])
 and any(c.get("type")=="Ready" and c.get("status")=="True" for c in p.get("status",{}).get("conditions",[]))))')
k -n messaging exec "$broker" -c kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server vietpulse-kafka-bootstrap:9092 --topic "$topic" --partition "$partition" \
    --offset "$offset" --max-messages 1 --timeout-ms 30000 > "$work_dir/record.json"
PYTHONPATH="$work_dir" python3 "$work_dir/checks.py" receipt "$work_dir/receipt.log" --record "$work_dir/record.json" >/dev/null
printf 'PASS: real crawler envelope matched Kafka %s partition %s offset %s on Node 3.\n' "$topic" "$partition" "$offset"
REMOTE
fi
if $check_pipelines; then
    on_node 1 <<'REMOTE' > "$work_dir/resources.json"
for workload in deployment/spark-operator-controller deployment/spark-operator-webhook; do
    k -n processing rollout status "$workload" --timeout=300s >&2
done
for app in raw-ingest speed-sentiment; do
    deadline=$((SECONDS + 360))
    until [[ $(k -n processing get sparkapplication "$app" -o jsonpath='{.status.applicationState.state}') == RUNNING ]]; do
        state=$(k -n processing get sparkapplication "$app" -o jsonpath='{.status.applicationState.state}')
        case "$state" in FAILED|SUBMISSION_FAILED|FAILING) fail "$app failed: inspect its driver/image errors" ;; esac
        ((SECONDS < deadline)) || fail "$app did not become RUNNING; check application artifacts"
        sleep 5
    done
done
directory=$(mktemp -d)
trap 'rm -rf -- "$directory"' EXIT
k -n processing get sparkapplications,deployments,pods,configmaps,cronjobs -o json > "$directory/processing.json"
k -n ingestion get cronjobs -o json > "$directory/ingestion.json"
python3 - "$directory/processing.json" "$directory/ingestion.json" <<'PY'
import json,sys
print(json.dumps({'items':sum((json.load(open(path))['items'] for path in sys.argv[1:]),[])}))
PY
REMOTE
    PYTHONPATH="$repo_root" python3 "$repo_root/infra/scripts/lib/processing_checks.py" snapshot "$work_dir/resources.json" \
        --spark-version "$spark_version" --operator-version "$operator_version"
    on_node 1 <<'REMOTE'
job="vp-batch-check-$(cat /proc/sys/kernel/random/uuid)"
directory=$(mktemp -d)
trap 'rm -rf -- "$directory"' EXIT
k -n processing get cronjob/vietpulse-batch -o json > "$directory/template.json"
python3 - "$directory/template.json" "$job" <<'PY' | k -n processing create -f -
import json,sys
spec=json.load(open(sys.argv[1]))['spec']['jobTemplate']['spec']
spec.update(activeDeadlineSeconds=900,backoffLimit=0)
print(json.dumps({'apiVersion':'batch/v1','kind':'Job','metadata':{'name':sys.argv[2],
 'labels':{'vietpulse.io/probe':'processing'}},'spec':spec}))
PY
deadline=$((SECONDS + 960))
until [[ $(k -n processing get job "$job" -o jsonpath='{.status.succeeded}') == 1 ]]; do
    failed=$(k -n processing get job "$job" -o jsonpath='{.status.failed}')
    [[ ${failed:-0} == 0 ]] || fail "Batch check failed; inspect job/$job and its Spark children. Completed UTC-date raw data is required"
    ((SECONDS < deadline)) || fail "Batch check timed out; inspect job/$job and the heavy-run lock"
    sleep 5
done
k -n processing logs "job/$job" > "$directory/result.log"
python3 - "$directory/result.log" <<'PY' > "$directory/apps.txt"
import json,sys
events=[]
for line in open(sys.argv[1]):
 try: event=json.loads(line)
 except ValueError: continue
 if event.get('state')=='COMPLETED': events.append(event)
assert [e['stage'] for e in events]==['batch-clean','batch-analytics','batch-publish'], 'Batch sequence incomplete'
print('\n'.join(e['application'] for e in events))
PY
while read -r app; do
    driver=$(k -n processing get sparkapplication "$app" -o jsonpath='{.status.driverInfo.podName}')
    [[ $(k -n processing get pod "$driver" -o jsonpath='{.spec.nodeName}') == vp-node-2 ]] || fail 'Batch driver ran outside Node 2'
done < "$directory/apps.txt"
printf 'PASS: both streaming jobs ready; clean, analytics and publish completed sequentially.\n'
REMOTE
fi
printf 'PASS: selected ingestion/processing checks completed.\n'
