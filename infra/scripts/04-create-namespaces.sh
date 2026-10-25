#!/usr/bin/env bash
set +xv
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
foundation_init "$@"
if $dry_run; then
    printf 'DRY RUN: verify the four-node cluster; create ingestion messaging storage processing serving monitoring through Node 1.\n'
    exit 0
fi
cluster_gate
on_node 1 <<'REMOTE'
for namespace in ingestion messaging storage processing serving monitoring; do
    printf 'Ensuring namespace %s\n' "$namespace"
    k create namespace "$namespace" --dry-run=client -o yaml | k apply -f -
done
REMOTE
printf 'PASS: six namespaces exist.\n'
