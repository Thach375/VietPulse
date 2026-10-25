#!/usr/bin/env bash
set +xv
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
foundation_init "$@"
if $dry_run; then
    printf 'DRY RUN: verify MCK/ECK, pinned databases on Node 4, PVCs, authentication and Kibana health.\n'
    printf 'DRY RUN: write isolated markers; restart MongoDB and Elasticsearch separately; read markers without rewriting.\n'
    exit 0
fi
command -v python3 >/dev/null || fail 'Python 3 and development dependencies are required on the operator machine'
work_dir=$(mktemp -d)
trap 'rm -rf -- "$work_dir"' EXIT
cluster_gate
on_node 1 <<'REMOTE' > "$work_dir/resources.json"
for workload in deployment/mongodb-kubernetes-operator statefulset/elastic-operator statefulset/vietpulse-mongodb statefulset/vietpulse-es-es-data deployment/vietpulse-kibana-kb; do
    k -n serving rollout status "$workload" --timeout=300s >&2
done
k -n serving wait --for=jsonpath='{.status.phase}'=Running mongodbcommunity/vietpulse-mongodb --timeout=300s >&2
k -n serving get mongodbcommunity,elasticsearch,kibana,pod,pvc,service -o json
REMOTE
python3 "$repo_root/infra/scripts/lib/serving_checks.py" resources "$work_dir/resources.json" \
    --pins "$repo_root/infra/versions.yaml" > "$work_dir/pods.json"
mapfile -t pods < <(python3 -c 'import json,sys; p=json.load(open(sys.argv[1])); print(p["mongodb"]); print(p["elasticsearch"])' "$work_dir/pods.json")
checks=$(base64 -w0 "$repo_root/infra/scripts/lib/serving_checks.py")
on_node 1 "$checks" "$(version_pin serving mongodb_probe_image)" "$(version_pin serving http_probe_image)" "${pods[0]}" "${pods[1]}" <<'REMOTE'
command -v python3 >/dev/null || fail 'Python 3 is required on Node 1'
work_dir=$(mktemp -d)
chmod 700 "$work_dir"
printf '%s' "$1" | base64 -d > "$work_dir/checks.py"
mongo_image=$2
http_image=$3
mongo_pod=$4
es_pod=$5
marker="vp-serving-$(cat /proc/sys/kernel/random/uuid)"
probe="vp-serving-probe-$(cat /proc/sys/kernel/random/uuid)"
cleanup() {
    status=$?
    trap - EXIT
    k -n serving delete pod "$probe" --ignore-not-found --wait=true --timeout=60s >/dev/null || true
    rm -rf -- "$work_dir"
    exit "$status"
}
trap cleanup EXIT
k -n serving create -f - <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $probe
  labels: {vietpulse.io/probe: serving}
spec:
  restartPolicy: Never
  activeDeadlineSeconds: 1200
  automountServiceAccountToken: false
  nodeSelector: {vietpulse.io/control: "true"}
  securityContext: {runAsUser: 1000, runAsGroup: 1000, fsGroup: 1000}
  containers:
    - name: mongo
      image: $mongo_image
      command: [sh, -c, 'sleep 1200']
      env:
        - {name: HOME, value: /tmp}
        - {name: VP_MONGO_CONFIG, value: /tmp/mongo-probe.json}
      resources:
        requests: {cpu: 50m, memory: 128Mi}
        limits: {cpu: "1", memory: 384Mi}
      volumeMounts:
        - {name: scripts, mountPath: /probes, readOnly: true}
        - {name: mongo-password, mountPath: /mongo, readOnly: true}
    - name: http
      image: $http_image
      command: [sh, -c, 'sleep 1200']
      env:
        - {name: VP_ES_PASSWORD_FILE, value: /elastic/elastic}
        - {name: VP_ES_CA, value: /es-ca/tls.crt}
        - {name: VP_KIBANA_CA, value: /kibana-ca/tls.crt}
        - {name: VP_ES_URL, value: https://vietpulse-es-es-http.serving.svc:9200}
        - {name: VP_KIBANA_URL, value: https://vietpulse-kibana-kb-http.serving.svc:5601}
      resources:
        requests: {cpu: 25m, memory: 16Mi}
        limits: {cpu: 200m, memory: 64Mi}
      volumeMounts:
        - {name: scripts, mountPath: /probes, readOnly: true}
        - {name: es-password, mountPath: /elastic, readOnly: true}
        - {name: es-ca, mountPath: /es-ca, readOnly: true}
        - {name: kibana-ca, mountPath: /kibana-ca, readOnly: true}
  volumes:
    - {name: scripts, configMap: {name: serving-probes}}
    - name: mongo-password
      secret: {secretName: vietpulse-mongodb-password, defaultMode: 0440}
    - name: es-password
      secret: {secretName: vietpulse-es-es-elastic-user, defaultMode: 0440}
    - name: es-ca
      secret: {secretName: vietpulse-es-es-http-certs-public}
    - name: kibana-ca
      secret: {secretName: vietpulse-kibana-kb-http-certs-public}
YAML
k -n serving wait --for=condition=Ready "pod/$probe" --timeout=300s
mongo_config() {
    printf '{"uri":"mongodb://vietpulse-mongodb-0.vietpulse-mongodb-svc.serving.svc.cluster.local:27017/?directConnection=true&serverSelectionTimeoutMS=10000","passwordFile":"/mongo/password","marker":"%s","write":%s}' "$marker" "$1" | \
        k -n serving exec -i "$probe" -c mongo -- sh -c 'cat > /tmp/mongo-probe.json'
}
mongo_probe() { k -n serving exec "$probe" -c mongo -- timeout 45 mongosh --quiet --nodb /probes/mongo-probe.js; }
wait_mongo_probe() {
    local attempt
    for attempt in {1..30}; do
        if mongo_probe; then return 0; fi
        sleep 2
    done
    fail 'MongoDB marker check failed after waiting for the writable primary'
}
http_probe() {
    local kind=$1
    k -n serving exec "$probe" -c http -- sh /probes/elastic-probe.sh "$kind" "$marker" > "$work_dir/$kind.json"
    [[ $kind == write || $kind == unauth ]] || python3 "$work_dir/checks.py" "$kind" "$work_dir/$kind.json" --marker "$marker"
}
wait_kibana_probe() {
    local attempt
    for attempt in {1..30}; do
        if http_probe kibana; then return 0; fi
        sleep 2
    done
    fail 'Kibana did not reconnect to Elasticsearch'
}
mongo_config true
wait_mongo_probe
http_probe write
for kind in unauth health document search; do http_probe "$kind"; done
wait_kibana_probe
printf 'PASS: authenticated MongoDB read/write, Elasticsearch indexing/search and Kibana connectivity.\n'
# From this point on, only reads are allowed; re-inserting would hide data loss.
mongo_config false
for pod in "$mongo_pod" "$es_pod"; do
    printf 'Restarting %s alone; retaining all claims and database resources.\n' "$pod"
    k -n serving delete pod "$pod" --wait=true --timeout=90s
    k -n serving wait --for=create "pod/$pod" --timeout=300s
    k -n serving wait --for=condition=Ready "pod/$pod" --timeout=300s
    if [[ $pod == "$mongo_pod" ]]; then
        wait_mongo_probe
    else
        for kind in health document search; do http_probe "$kind"; done
        wait_kibana_probe
    fi
done
printf 'PASS: both database markers survived individual pod restarts.\n'
REMOTE
printf 'PASS: persistent serving database verification completed.\n'
