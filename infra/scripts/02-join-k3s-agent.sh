#!/usr/bin/env bash
set +xv
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
foundation_init "$@"
if $dry_run; then
    printf 'DRY RUN: verify server; '
    [[ $selected_node == 2 ]] || printf 'require Node 2 Ready with a successful two-node network/DNS test; '
    printf 'join only vp-node-%s (%s) using %s and a redacted token; verify readiness' "$selected_node" "${config[NODE${selected_node}_IP]}" "$k3s_version"
    [[ $selected_node != 2 ]] || printf ' and run the two-node test'
    printf '.\n'
    exit 0
fi
server_gate
if [[ $selected_node != 2 ]]; then
    node2_gate
fi
join_token=${K3S_TOKEN:-}
unset K3S_TOKEN
if [[ -z $join_token ]]; then
    [[ -r /dev/tty ]] || fail 'Set K3S_TOKEN in the environment or run with an interactive terminal'
    IFS= read -r -s -p 'K3s join token (hidden): ' join_token </dev/tty || fail 'Could not read join token'
    printf '\n' >&2
fi
[[ $join_token =~ ^[A-Za-z0-9_:.-]+$ ]] || fail 'Join token must be a nonempty, single-line K3s token'
install_node "$selected_node" agent
unset join_token
check_joined_node "$selected_node"
[[ $selected_node != 2 ]] || node2_gate
printf 'PASS: vp-node-%s joined and verified.\n' "$selected_node"
