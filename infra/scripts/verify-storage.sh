#!/usr/bin/env bash
set +xv
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
foundation_init "$@"
if $dry_run; then
    printf 'DRY RUN: check HDFS/Hive/PostgreSQL readiness, Node 2/4 placement and persistent claims.\n'
    printf 'DRY RUN: require two live DataNodes, replication 2, dataset/checkpoint directories, file round trip and catalog DDL.\n'
    exit 0
fi
command -v python3 >/dev/null || fail 'Python 3 is required on the operator machine'
work_dir=$(mktemp -d)
trap 'rm -rf -- "$work_dir"' EXIT
cluster_gate
on_node 1 <<'REMOTE' > "$work_dir/resources.json"
for workload in statefulset/hdfs-namenode statefulset/hdfs-datanode-batch statefulset/hdfs-datanode-data statefulset/hive-postgresql deployment/hive-metastore; do
    k -n storage rollout status "$workload" --timeout=300s >&2
done
k -n storage wait --for=condition=complete job/hdfs-bootstrap --timeout=300s >&2
k -n storage get pod,pvc -o json
REMOTE
python3 "$repo_root/infra/scripts/lib/storage_checks.py" resources "$work_dir/resources.json" \
    --hadoop-image "$(version_pin storage hadoop_image)" \
    --metastore-image "$(version_pin storage metastore_image)" \
    --postgresql-image "$(version_pin storage postgresql_image)"
on_node 1 <<'REMOTE' > "$work_dir/report.txt"
k -n storage exec hdfs-namenode-0 -- hdfs dfsadmin -report
REMOTE
python3 "$repo_root/infra/scripts/lib/storage_checks.py" report "$work_dir/report.txt"
on_node 1 <<'REMOTE'
k -n storage exec hdfs-namenode-0 -- timeout 240 bash /etc/hadoop/conf/probe-hdfs.sh
k -n storage exec deployment/hive-metastore -- timeout 180 env HIVE_CONF_DIR=/tmp/hive-conf \
    bash /etc/hive-scripts/probe-catalog.sh
REMOTE
printf 'PASS: HDFS storage and Hive catalog verification completed.\n'
