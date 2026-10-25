#!/usr/bin/env bash
set +xv
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
foundation_init "$@"
if $dry_run; then
    printf 'DRY RUN: verify exactly four Ready nodes, pinned versions/IPs, system workloads, role labels and six namespaces.\n'
    $check_network && printf 'DRY RUN: run DNS and all directed cross-node HTTP probes using %s.\n' "$verification_image"
    $check_storage && printf 'DRY RUN: verify storage paths; write/recreate/read a 16Mi PVC probe on each node; retain probe PVCs.\n'
    exit 0
fi
cluster_gate
for node in 1 2 3 4; do
    on_node 1 "$node" "$(node_labels "$node")" <<'REMOTE'
read -r -a labels <<< "$2"
for label in "${labels[@]}"; do
    found=$(k get nodes -l "$label" -o name)
    [[ " $found " == *"node/vp-node-$1"* ]] || fail "Missing $label on vp-node-$1"
done
REMOTE
done
on_node 1 <<'REMOTE'
for namespace in ingestion messaging storage processing serving monitoring; do
    state=$(k get namespace "$namespace" -o jsonpath='{.status.phase}')
    [[ $state == Active ]] || fail "Namespace $namespace is not Active"
done
REMOTE
verify_storage_config
if $check_network; then
    on_node 1 "$verification_image" <<'REMOTE'
network_test "$1" 1 2 3 4
REMOTE
fi
if $check_storage; then
    storage_test
fi
printf 'PASS: requested cluster foundation checks passed.\n'
