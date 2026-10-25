#!/usr/bin/env bash
set +xv
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
foundation_init "$@"
if $dry_run; then
    printf 'DRY RUN: verify the four-node cluster and apply the PRD role labels through Node 1.\n'
    for node in 1 2 3 4; do printf 'vp-node-%s: %s\n' "$node" "$(node_labels "$node")"; done
    exit 0
fi
cluster_gate
for node in 1 2 3 4; do
    labels=$(node_labels "$node")
    # Role labels are fixed repository values; only these keys may be changed.
    on_node 1 "$node" "$labels" <<'REMOTE'
target=$1
read -r -a labels <<< "$2"
printf 'Applying role labels to vp-node-%s\n' "$target"
k label node "vp-node-$target" "${labels[@]}" --overwrite
REMOTE
done
printf 'PASS: role labels applied.\n'
