#!/usr/bin/env bash
set +xv
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
foundation_init "$@"
kafka_version=$(version_pin messaging kafka)
operator_version=$(version_pin messaging operator_version)
if $dry_run; then
    printf 'DRY RUN: check four Ready nodes, Strimzi %s, Kafka %s, placement, PVCs, project topics and actual ISR.\n' "$operator_version" "$kafka_version"
    printf 'DRY RUN: check the retained verification marker, then produce/consume a fresh UUID in an isolated compacted topic.\n'
    exit 0
fi
command -v python3 >/dev/null || fail 'Python 3 is required on the operator machine'
work_dir=$(mktemp -d)
trap 'rm -rf -- "$work_dir"' EXIT
cluster_gate
on_node 1 <<'REMOTE' > "$work_dir/resources.json"
k -n messaging rollout status deployment/strimzi-cluster-operator --timeout=300s >&2
resources=$(k -n messaging get kafkatopic -l strimzi.io/cluster=vietpulse -o name)
[[ -n $resources ]] || fail 'No project KafkaTopic resources found; apply the multi-node overlay first'
while IFS= read -r resource; do
    generation=$(k -n messaging get "$resource" -o 'jsonpath={.metadata.generation}')
    k -n messaging wait --for="jsonpath={.status.observedGeneration}=$generation" "$resource" --timeout=300s >&2
    k -n messaging wait --for=condition=Ready "$resource" --timeout=300s >&2
done < <(printf 'kafka/vietpulse\n%s\n' "$resources")
k -n messaging get kafka,kafkanodepool,kafkatopic,pod,pvc,deployment,node -o json
REMOTE
broker=$(python3 "$repo_root/infra/scripts/lib/kafka_checks.py" resources "$work_dir/resources.json" \
    --kafka-version "$kafka_version" --operator-version "$operator_version")
[[ $broker =~ ^vietpulse-batch-[0-9]+$ ]] || fail 'Invalid verified broker pod name'
on_node 1 "$broker" <<'REMOTE' > "$work_dir/topics.txt"
k -n messaging exec "$1" -c kafka -- /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server vietpulse-kafka-bootstrap:9092 --describe --exclude-internal
REMOTE
python3 "$repo_root/infra/scripts/lib/kafka_checks.py" topics "$work_dir/topics.txt"
printf 'PASS: pinned Kafka/Strimzi, three broker/controller placements, PVCs and all project topics/ISRs.\n'
on_node 1 "$broker" <<'REMOTE'
broker=$1
topic=vietpulse.infra.verify
marker_store=vp-kafka-verification
bootstrap=vietpulse-kafka-bootstrap:9092
kafka_cli() { k -n messaging exec "$broker" -c kafka -- "/opt/kafka/bin/$1" --bootstrap-server "$bootstrap" "${@:2}"; }
# Only our labelled probe resources may be reconciled. Never write into raw.*.
for resource in "kafkatopic/$topic" "configmap/$marker_store"; do
    existing=$(k -n messaging get "$resource" --ignore-not-found -o name)
    if [[ -n $existing ]]; then
        owner=$(k -n messaging get "$resource" -o 'jsonpath={.metadata.labels.vietpulse\.io/probe}')
        [[ $owner == kafka ]] || fail "Probe name $resource is occupied by an unrelated resource"
    fi
done
k -n messaging apply -f - <<'TOPIC'
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: vietpulse.infra.verify
  labels:
    strimzi.io/cluster: vietpulse
    vietpulse.io/probe: kafka
spec:
  partitions: 1
  replicas: 3
  config:
    min.insync.replicas: 2
    cleanup.policy: compact
    segment.bytes: 1048576
TOPIC
k -n messaging wait --for=condition=Ready "kafkatopic/$topic" --timeout=300s
consume_marker() {
    local offset=$1 expected=$2 actual
    actual=$(kafka_cli kafka-console-consumer.sh --topic "$topic" --partition 0 --offset "$offset" \
        --max-messages 1 --timeout-ms 30000)
    [[ $actual == "$expected" ]] || fail 'Verification marker was lost or changed; inspect Kafka storage before continuing'
}
existing=$(k -n messaging get configmap "$marker_store" --ignore-not-found -o name)
if [[ -n $existing ]]; then
    previous_offset=$(k -n messaging get configmap "$marker_store" -o 'jsonpath={.data.offset}')
    previous_marker=$(k -n messaging get configmap "$marker_store" -o 'jsonpath={.data.marker}')
    [[ $previous_offset =~ ^[0-9]+$ && $previous_marker =~ ^vp-verify-[a-f0-9-]{36}$ ]] || fail 'Invalid retained marker metadata'
    consume_marker "$previous_offset" "$previous_marker"
    printf 'PASS: previous verification marker survived since the last run.\n'
fi
offset=$(kafka_cli kafka-get-offsets.sh --topic "$topic" --time latest | awk -F: '$2 == 0 {print $3}')
[[ $offset =~ ^[0-9]+$ ]] || fail 'Could not read verification topic end offset'
marker=vp-verify-$(cat /proc/sys/kernel/random/uuid)
# A constant key lets compaction retain the latest successful marker indefinitely.
printf 'sentinel:%s\n' "$marker" | k -n messaging exec -i "$broker" -c kafka -- \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$bootstrap" --topic "$topic" \
    --property parse.key=true --property key.separator=: \
    --producer-property acks=all --producer-property enable.idempotence=true \
    --producer-property delivery.timeout.ms=60000 --producer-property request.timeout.ms=15000
consume_marker "$offset" "$marker"
k -n messaging create configmap "$marker_store" --from-literal="offset=$offset" --from-literal="marker=$marker" \
    --dry-run=client -o yaml | \
    k label --local -f - vietpulse.io/probe=kafka -o yaml | k -n messaging apply -f -
printf 'PASS: produced and consumed unique marker %s at partition 0 offset %s.\n' "$marker" "$offset"
REMOTE
printf 'PASS: Kafka verification completed; probe topic, marker and persistent data are retained.\n'
