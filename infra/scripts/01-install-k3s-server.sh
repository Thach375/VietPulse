#!/usr/bin/env bash
set +xv
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
foundation_init "$@"
if $dry_run; then
    printf 'DRY RUN: install %s only on vp-node-1 (%s); verify server and bundled components; configure per-node storage.\n' "$k3s_version" "${config[NODE1_IP]}"
    for node in 1 2 3 4; do
        printf 'Storage mapping: vp-node-%s -> %s/vietpulse/local-path\n' "$node" "${config[NODE${node}_STORAGE_PATH]%/}"
    done
    exit 0
fi
install_node 1 server
server_gate
configure_storage
printf 'PASS: server foundation ready; next join Node 2 explicitly.\n'
