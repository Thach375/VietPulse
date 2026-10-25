#!/usr/bin/env bash
# Inspect the four explicitly configured hosts. This script never prepares them.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
usage() {
    printf 'Usage: bash infra/scripts/00-preflight.sh --env-file FILE --read-only\n'
}

env_file=
read_only=false
while (($#)); do
    case "$1" in
        --env-file)
            (($# >= 2)) || fail '--env-file requires a file'
            env_file=$2
            shift 2
            ;;
        --read-only) read_only=true; shift ;;
        --help|-h) usage; exit 0 ;;
        *) fail 'Unknown argument; use --help' ;;
    esac
done
[[ $read_only == true ]] || fail '--read-only is required'
[[ -n $env_file && -f $env_file && -r $env_file ]] || fail '--env-file must name a readable file'

load_inventory
printf 'Read-only inventory: four nodes; firewall policy=%s\n' "$policy"

for node in 1 2 3 4; do
    address=${config[NODE${node}_IP]}
    username=${config[NODE${node}_USER]}
    storage_path=${config[NODE${node}_STORAGE_PATH]}
    printf '\nNode %s: %s@%s (expected vp-node-%s)\n' "$node" "$username" "$address" "$node"
    # Every interpolated argument below has been restricted to safe characters.
    if timeout 60 ssh "${ssh_options[@]}" "$username@$address" \
        "bash -s -- $node $storage_path $policy ${ips[*]}" <<'REMOTE'
set -euo pipefail
export LC_ALL=C
node=$1
storage_path=$2
policy=$3
shift 3
ips=("$@")
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
for command in hostname awk lscpu free df findmnt ip ping sudo ufw timeout; do
    command -v "$command" >/dev/null || fail "Missing host command: $command; prepare Ubuntu manually"
done
actual_hostname=$(hostname)
printf 'Hostname: %s\n' "$actual_hostname"
[[ $actual_hostname == "vp-node-$node" ]] || fail "Expected hostname vp-node-$node; correct the inventory or prepare the host manually"
os_id=$(awk -F= '$1 == "ID" {gsub(/"/, "", $2); print $2}' /etc/os-release)
os_version=$(awk -F= '$1 == "VERSION_ID" {gsub(/"/, "", $2); print $2}' /etc/os-release)
printf 'OS: %s %s\n' "$os_id" "$os_version"
[[ $os_id == ubuntu && ( $os_version == 22.04 || $os_version == 24.04 ) ]] || fail 'Supported host baseline: Ubuntu 22.04 or 24.04 LTS'
printf '\nCPU inventory:\n'
lscpu
printf '\nRAM inventory (MiB):\n'
free -m
[[ -d $storage_path ]] || fail 'Configured storage path must already exist; no directories will be created'
printf '\nFilesystem backing %s:\n' "$storage_path"
timeout 5 findmnt --target "$storage_path" --output SOURCE,FSTYPE,TARGET
timeout 5 df -Pk -- "$storage_path"
printf '\nNetwork interfaces:\n'
ip -br address
for peer in "${ips[@]}"; do
    [[ $peer == "${ips[node-1]}" ]] && continue
    ip route get "$peer"
    ping -n -c 1 -W 2 "$peer" >/dev/null || fail "Cannot reach peer $peer (ICMP is required for this preflight)"
    printf 'PASS: peer %s reachable\n' "$peer"
done
printf '\nRead-only firewall status:\n'
firewall=$(sudo -n ufw status verbose) || fail 'Cannot read firewall status; arrange noninteractive sudo permission for ufw status only'
printf '%s\n' "$firewall"
case "$policy" in
    ufw-disabled) [[ $firewall == 'Status: inactive'* ]] || fail 'Chosen firewall policy requires inactive UFW; no policy was changed' ;;
    ufw-rules) [[ $firewall == 'Status: active'* ]] || fail 'Chosen firewall policy requires active UFW; no policy was changed' ;;
esac
printf 'UNVERIFIED: K3s TCP 6443/10250 and UDP 8472; review rules and verify after installation.\n'
printf 'PASS: read-only host inventory collected\n'
REMOTE
    then
        :
    else
        status=$?
        fail "Node $node preflight failed (exit $status); stopping. Check the preceding probe/SSH error."
    fi
done
printf '\nPASS: all four inventories collected; no host configuration was changed.\n'
printf 'This is an inventory gate, not proof of K3s port access, storage throughput or workload capacity.\n'
