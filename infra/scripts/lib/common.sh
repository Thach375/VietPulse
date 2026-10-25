#!/usr/bin/env bash
# Shared literal inventory validation and trusted SSH settings.
# Flags below are consumed by the scripts that source this file.
# shellcheck disable=SC2034

fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

load_inventory() {
    # Treat the inventory as data, never as a shell program. Do not echo its values
    # on validation errors: an accidentally pasted secret must not enter logs.
    declare -gA config=() seen_ips=()
    line_number=0
    while IFS= read -r line || [[ -n $line ]]; do
        ((line_number += 1))
        line=${line%$'\r'}
        [[ $line =~ ^[[:space:]]*(#.*)?$ ]] && continue
        [[ $line =~ ^([A-Z][A-Z0-9_]*)=(.*)$ ]] || fail "Invalid assignment at line $line_number"
        key=${BASH_REMATCH[1]}
        value=${BASH_REMATCH[2]}
        case "$key" in
            NODE[1-4]_IP|NODE[1-4]_USER|NODE[1-4]_STORAGE_PATH|FIREWALL_POLICY|SSH_PORT|SSH_IDENTITY_FILE) ;;
            *) fail "Unknown configuration key at line $line_number" ;;
        esac
        [[ ! -v config[$key] ]] || fail "Duplicate configuration key at line $line_number"
        config[$key]=$value
    done < "$env_file"

    valid_ip() {
        local address=$1 octet
        local -a octets
        [[ $address =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]] || return 1
        IFS=. read -r -a octets <<< "$address"
        for octet in "${octets[@]}"; do
            [[ $octet == 0 || $octet != 0* ]] || return 1
            ((10#$octet <= 255)) || return 1
        done
        # The deployment is confined to a trusted RFC 1918 LAN.
        [[ ${octets[0]} == 10 ]] ||
            [[ ${octets[0]} == 192 && ${octets[1]} == 168 ]] ||
            [[ ${octets[0]} == 172 && ${octets[1]} -ge 16 && ${octets[1]} -le 31 ]]
    }

    for node in 1 2 3 4; do
        for field in IP USER STORAGE_PATH; do
            key=NODE${node}_${field}
            [[ -n ${config[$key]:-} ]] || fail "$key is required"
        done
        key=NODE${node}_IP
        address=${config[$key]}
        valid_ip "$address" || fail "$key must be a private, canonical IPv4 address"
        [[ ! -v seen_ips[$address] ]] || fail "$key duplicates another node IP"
        seen_ips[$address]=1
        key=NODE${node}_USER
        [[ ${config[$key]} =~ ^[a-z_][a-z0-9_-]*$ ]] || fail "$key must be a Linux username"
        key=NODE${node}_STORAGE_PATH
        [[ ${config[$key]} =~ ^/[a-zA-Z0-9_./-]*$ ]] || fail "$key must be an absolute path without shell syntax or spaces"
    done
    policy=${config[FIREWALL_POLICY]:-}
    [[ $policy == ufw-rules || $policy == ufw-disabled ]] || fail 'FIREWALL_POLICY must be ufw-rules or ufw-disabled'
    port=${config[SSH_PORT]:-22}
    if [[ ! $port =~ ^[1-9][0-9]{0,4}$ ]] || ((port > 65535)); then
        fail 'SSH_PORT must be between 1 and 65535'
    fi
    identity=${config[SSH_IDENTITY_FILE]:-}
    if [[ -n $identity ]]; then
        [[ $identity == /* && -f $identity && -r $identity ]] || fail 'SSH_IDENTITY_FILE must name a readable absolute file'
    fi
    for command in ssh timeout; do
        command -v "$command" >/dev/null || fail "Missing local command: $command"
    done

    # Known host keys and working key/agent authentication are human prerequisites.
    # Ignore SSH config hooks, forwarding and connection sharing; never enroll keys.
    ssh_options=(-F /dev/null -T -p "$port"
        -o BatchMode=yes -o StrictHostKeyChecking=yes -o UpdateHostKeys=no
        -o PermitLocalCommand=no -o ClearAllForwardings=yes
        -o ControlMaster=no -o ControlPath=none
        -o ConnectTimeout=5 -o ConnectionAttempts=1
        -o ServerAliveInterval=5 -o ServerAliveCountMax=2)
    [[ -z $identity ]] || ssh_options+=(-i "$identity" -o IdentitiesOnly=yes)
    ips=()
    for node in 1 2 3 4; do ips+=("${config[NODE${node}_IP]}"); done
}

foundation_init() {
    repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
    env_file=$repo_root/infra/.env
    local script=${0##*/}
    selected_node=
    dry_run=false
    check_network=false
    check_storage=false
    while (($#)); do
        case "$1" in
            --env-file)
                (($# >= 2)) || fail '--env-file requires a file'
                env_file=$2; shift 2 ;;
            --node)
                (($# >= 2)) || fail '--node requires a node number'
                [[ -z $selected_node ]] || fail 'Select exactly one --node per invocation'
                selected_node=$2; shift 2 ;;
            --dry-run) dry_run=true; shift ;;
            --check-network|--check-storage)
                [[ $script == verify-cluster.sh ]] || fail 'Check options belong to verify-cluster.sh'
                if [[ $1 == --check-network ]]; then check_network=true; else check_storage=true; fi
                shift ;;
            --help|-h)
                printf 'Usage: bash %s [--env-file FILE] [--dry-run]' "$script"
                case "$script" in
                    01-*) printf ' --node 1' ;;
                    02-*) printf ' --node {2|3|4}' ;;
                    verify-cluster.sh) printf ' [--check-network] [--check-storage]' ;;
                esac
                printf '\nDefault inventory: infra/.env; dry runs never connect to hosts.\n'
                exit 0 ;;
            *) fail 'Unknown argument; use --help' ;;
        esac
    done
    case "$script" in
        01-*) [[ $selected_node == 1 ]] || fail 'Server installation requires --node 1' ;;
        02-*) [[ $selected_node =~ ^[234]$ ]] || fail 'Agent installation requires --node 2, 3, or 4' ;;
        *) [[ -z $selected_node ]] || fail '--node is only supported by installation scripts' ;;
    esac
    [[ -f $env_file && -r $env_file ]] || fail '--env-file must name a readable file; fill infra/.env from infra/.env.example'
    load_inventory
    k3s_version=$(foundation_pin k3s)
    verification_image=$(foundation_pin verification_image)
    [[ $k3s_version =~ ^v[0-9]+\.[0-9]+\.[0-9]+\+k3s[0-9]+$ ]] || fail 'Invalid foundation.k3s pin'
    [[ $verification_image =~ ^[a-z0-9./_-]+:[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail 'Verification image requires an exact version tag'
    [[ $(foundation_pin flannel_backend) == vxlan ]] || fail 'Only the configured VXLAN foundation is supported'
}

# Deliberately read only quoted, two-space version scalars, not arbitrary YAML.
version_pin() {
    awk -v section_name="$1" -v key="$2" '
        $0 == section_name ":" {section=1; next}
        /^[^ #]/ {section=0}
        section && $0 ~ "^  " key ": \"[^\"]+\"$" {
            value=$0; sub(/^[^:]+: "/, "", value); sub(/"$/, "", value); count++
        }
        END {if (count != 1) exit 1; print value}
    ' "$repo_root/infra/versions.yaml" || fail "Missing or duplicate $1.$2 pin"
}

foundation_pin() { version_pin foundation "$1"; }

node_labels() {
    case "$1" in
        1) printf '%s\n' 'vietpulse.io/control=true vietpulse.io/monitoring=true vietpulse.io/serving=true' ;;
        2) printf '%s\n' 'vietpulse.io/batch=true vietpulse.io/storage=true vietpulse.io/kafka=true' ;;
        3) printf '%s\n' 'vietpulse.io/streaming=true vietpulse.io/ingestion=true vietpulse.io/kafka=true' ;;
        4) printf '%s\n' 'vietpulse.io/data=true vietpulse.io/storage=true vietpulse.io/compute=true vietpulse.io/kafka=true' ;;
    esac
}

# Send the remote program over encrypted stdin. Arguments are shell-quoted;
# secrets may appear only in stdin, never in the SSH command or diagnostic text.
on_node() {
    local target=$1 command
    shift
    printf -v command '%q ' sudo -n bash -s -- "$target" "$@"
    {
        remote_helpers
        cat
    } | timeout 1200 ssh "${ssh_options[@]}" "${config[NODE${target}_USER]}@${config[NODE${target}_IP]}" "$command"
}

remote_helpers() {
    cat <<'REMOTE'
set +xv
set -Eeuo pipefail
export LC_ALL=C
node=$1
shift
fail() { printf 'ERROR: Node %s: %s\n' "$node" "$*" >&2; exit 1; }
trap 'printf "ERROR: Node %s operation failed at remote line %s (exit %s).\n" "$node" "$LINENO" "$?" >&2' ERR
[[ $(hostname) == "vp-node-$node" ]] || fail 'Unexpected hostname; check inventory before changing hosts'
k() { /usr/local/bin/k3s kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml --request-timeout=330s "$@"; }
check_node() {
    local target=$1 expected_ip=$2 expected_version=$3 actual
    k wait --for=create "node/vp-node-$target" --timeout=300s
    k wait --for=condition=Ready "node/vp-node-$target" --timeout=300s
    actual=$(k get node "vp-node-$target" -o jsonpath='{.status.nodeInfo.kubeletVersion}{"|"}{.status.addresses[?(@.type=="InternalIP")].address}')
    [[ $actual == "$expected_version|$expected_ip" ]] || fail "Node $target version or InternalIP differs from inventory/pin"
}
check_system() {
    [[ $(k get --raw=/readyz) == ok ]] || fail 'Kubernetes API is not ready'
    for deployment in coredns local-path-provisioner metrics-server traefik; do
        k -n kube-system wait --for=create "deployment/$deployment" --timeout=300s
        k -n kube-system rollout status "deployment/$deployment" --timeout=300s
    done
}
probe_pods=()
cleanup_probes() {
    local status=${1:-$?} pod
    trap - EXIT
    for pod in "${probe_pods[@]}"; do
        k -n kube-system delete pod "$pod" --ignore-not-found --wait=true --timeout=60s >/dev/null || true
    done
    exit "$status"
}
trap cleanup_probes EXIT
probe_id="$(date +%s)-$$"
create_probe_pod() {
    local name=$1 target=$2 image=$3 claim=${4:-} volume='' mount='' command
    command='["sh","-ec","printf vp-node-'"$target"' > /tmp/index.html; exec httpd -f -p 8080 -h /tmp"]'
    if [[ -n $claim ]]; then
        volume=',"volumes":[{"name":"data","persistentVolumeClaim":{"claimName":"'"$claim"'"}}]'
        mount=',"volumeMounts":[{"name":"data","mountPath":"/data"}]'
    fi
    k create -f - <<JSON
{"apiVersion":"v1","kind":"Pod","metadata":{"name":"$name","namespace":"kube-system","labels":{"vietpulse.io/probe":"k3s-foundation"}},"spec":{"restartPolicy":"Never","activeDeadlineSeconds":600,"automountServiceAccountToken":false,"nodeSelector":{"kubernetes.io/hostname":"vp-node-$target"},"containers":[{"name":"probe","image":"$image","command":$command,"resources":{"requests":{"cpu":"5m","memory":"8Mi"},"limits":{"cpu":"100m","memory":"32Mi"}},"readinessProbe":{"httpGet":{"path":"/","port":8080},"periodSeconds":1}$mount}]$volume}}
JSON
    probe_pods+=("$name")
    k -n kube-system wait --for=condition=Ready "pod/$name" --timeout=300s
    [[ $(k -n kube-system get pod "$name" -o jsonpath='{.spec.nodeName}') == "vp-node-$target" ]] || fail 'Probe scheduled on an unexpected node'
}
network_test() {
    local image=$1 source target name response
    shift
    local -a targets=("$@")
    local -A pods=() addresses=()
    for target in "${targets[@]}"; do
        name="vp-foundation-net-$probe_id-$target"
        create_probe_pod "$name" "$target" "$image"
        pods[$target]=$name
        addresses[$target]=$(k -n kube-system get pod "$name" -o jsonpath='{.status.podIP}')
        [[ -n ${addresses[$target]} ]] || fail 'Probe has no pod IP'
        k -n kube-system exec "$name" -- nslookup kubernetes.default.svc.cluster.local >/dev/null
    done
    for source in "${targets[@]}"; do
        for target in "${targets[@]}"; do
            [[ $source == "$target" ]] && continue
            response=$(k -n kube-system exec "${pods[$source]}" -- wget -q -T 10 -O - "http://${addresses[$target]}:8080/")
            [[ $response == "vp-node-$target" ]] || fail "Cross-node HTTP check failed: $source -> $target"
        done
    done
    printf 'PASS: DNS on every probe node and all directed cross-node HTTP checks.\n'
}
REMOTE
}

server_gate() {
    on_node 1 "$k3s_version" "${config[NODE1_IP]}" <<'REMOTE'
systemctl is-active --quiet k3s || fail 'Server service is not active; install/verify Node 1 first'
deadline=$((SECONDS + 300))
until k --request-timeout=5s get --raw=/readyz >/dev/null 2>&1; do
    ((SECONDS < deadline)) || fail 'API did not become ready within five minutes'
    sleep 2
done
check_node 1 "$2" "$1"
check_system
REMOTE
}

check_joined_node() {
    on_node 1 "$1" "${config[NODE${1}_IP]}" "$k3s_version" <<'REMOTE'
check_node "$1" "$2" "$3"
REMOTE
}

node2_gate() {
    check_joined_node 2
    on_node 1 "$verification_image" <<'REMOTE'
network_test "$1" 1 2
REMOTE
}

cluster_gate() {
    server_gate
    for node in 2 3 4; do check_joined_node "$node"; done
    on_node 1 <<'REMOTE'
actual=$(k get nodes -o name | sort)
expected=$(printf 'node/vp-node-%s\n' 1 2 3 4)
[[ $actual == "$expected" ]] || fail 'Expected exactly vp-node-1 through vp-node-4; check unexpected/missing nodes'
REMOTE
}

install_node() {
    local target=$1 role=$2
    printf 'Installing/verifying %s %s only on vp-node-%s (%s).\n' "$role" "$k3s_version" "$target" "${config[NODE${target}_IP]}"
    {
        # xtrace is disabled by every entry script and remote_helpers before this
        # assignment is evaluated. The token is never a process argument.
        printf 'join_token=%q\n' "${join_token:-}"
        cat <<'REMOTE'
role=$1
version=$2
address=$3
storage_path=$4
policy=$5
server_address=$6
for command in curl systemctl ip ufw swapon findmnt df; do
    command -v "$command" >/dev/null || fail "Missing $command; complete Ubuntu preparation first"
done
os_id=$(awk -F= '$1 == "ID" {gsub(/"/, "", $2); print $2}' /etc/os-release)
os_version=$(awk -F= '$1 == "VERSION_ID" {gsub(/"/, "", $2); print $2}' /etc/os-release)
[[ $os_id == ubuntu && ( $os_version == 22.04 || $os_version == 24.04 ) ]] || fail 'Prepare Ubuntu 22.04 or 24.04 LTS first'
[[ -z $(swapon --show --noheadings) ]] || fail 'Active swap detected; complete Ubuntu swap preparation manually'
ip -4 -o address show | awk -v address="$address" '{split($4, parts, "/"); if (parts[1] == address) found=1} END {exit !found}' || fail 'Inventory IP is not assigned to this host'
[[ -d $storage_path ]] || fail 'Configured storage directory does not exist'
findmnt --target "$storage_path" --output SOURCE,FSTYPE,TARGET
df -Pk -- "$storage_path"
firewall=$(ufw status verbose)
case "$policy" in
    ufw-disabled) [[ $firewall == 'Status: inactive'* ]] || fail 'UFW state differs from chosen policy' ;;
    ufw-rules) [[ $firewall == 'Status: active'* ]] || fail 'UFW state differs from chosen policy' ;;
esac
desired=$(mktemp)
installer=$(mktemp)
cleanup_install() {
    local status=$?
    rm -f -- "$desired" "$installer"
    cleanup_probes "$status"
}
trap cleanup_install EXIT
chmod 600 "$desired" "$installer"
cat > "$desired" <<YAML
# Managed by VietPulse Task 2; conflicting configurations require operator review.
node-name: "vp-node-$node"
node-ip: "$address"
YAML
service=k3s
if [[ $role == server ]]; then
    cat >> "$desired" <<YAML
write-kubeconfig-mode: "0600"
tls-san: ["$address"]
flannel-backend: "vxlan"
default-local-storage-path: "${storage_path%/}/vietpulse/local-path"
YAML
else
    service=k3s-agent
    cat >> "$desired" <<YAML
server: "https://$server_address:6443"
token-file: "/etc/rancher/k3s/vietpulse-agent-token"
YAML
fi
if [[ -e /etc/rancher/k3s/config.yaml ]]; then
    cmp -s "$desired" /etc/rancher/k3s/config.yaml || fail 'Existing K3s configuration differs; refusing to overwrite state'
    [[ ! -d /etc/rancher/k3s/config.yaml.d ]] || fail 'Existing configuration drop-ins require operator review'
    [[ -x /usr/local/bin/k3s ]] || fail 'Partial installation found; inspect it manually before retrying'
    installed=$(/usr/local/bin/k3s --version | awk 'NR==1 {print $3}')
    [[ $installed == "$version" ]] || fail 'Installed K3s version differs from pin; no upgrade was attempted'
    if [[ $role == agent ]]; then
        [[ -f /etc/rancher/k3s/vietpulse-agent-token ]] || fail 'Existing agent token file is missing'
        [[ $(cat /etc/rancher/k3s/vietpulse-agent-token) == "$join_token" ]] || fail 'Join token differs from existing agent; no token was changed'
    fi
    systemctl is-active --quiet "$service" || fail 'Matching installation is inactive; inspect/start it manually'
    printf 'Matching active installation reused without running the installer.\n'
else
    for existing in /usr/local/bin/k3s /var/lib/rancher/k3s /etc/systemd/system/k3s.service /etc/systemd/system/k3s-agent.service /etc/rancher/k3s; do
        [[ ! -e $existing && ! -L $existing ]] || fail 'Unmanaged or partial K3s installation exists; inspect it manually'
    done
    # Download completely before creating configuration or running privileged code.
    curl --fail --silent --show-error --location --connect-timeout 10 --max-time 120 -o "$installer" https://get.k3s.io
    [[ -s $installer ]] || fail 'Empty installer download'
    install -d -m 700 /etc/rancher/k3s
    install -m 600 "$desired" /etc/rancher/k3s/config.yaml
    if [[ $role == agent ]]; then
        umask 077
        printf '%s\n' "$join_token" > /etc/rancher/k3s/vietpulse-agent-token
    fi
    unset join_token K3S_TOKEN K3S_URL K3S_CONFIG_FILE
    # Installer validates the pinned release binary against its release checksum.
    INSTALL_K3S_VERSION="$version" INSTALL_K3S_EXEC="$role" INSTALL_K3S_SYMLINK=skip sh "$installer"
    systemctl is-active --quiet "$service" || fail 'Installed K3s service is not active'
fi
REMOTE
    } | on_node "$target" "$role" "$k3s_version" "${config[NODE${target}_IP]}" "${config[NODE${target}_STORAGE_PATH]}" "$policy" "${config[NODE1_IP]}"
}

storage_config_json() {
    local target separator=
    printf '{"nodePathMap":['
    for target in 1 2 3 4; do
        printf '%s{"node":"vp-node-%s","paths":["%s/vietpulse/local-path"]}' "$separator" "$target" "${config[NODE${target}_STORAGE_PATH]%/}"
        separator=,
    done
    printf ']}'
}

configure_storage() {
    on_node 1 "$(storage_config_json)" "${config[NODE1_STORAGE_PATH]%/}/vietpulse/local-path" <<'REMOTE'
desired=$1
default='{"nodePathMap":[{"node":"DEFAULT_PATH_FOR_NON_LISTED_NODES","paths":["'"$2"'"]}]}'
k -n kube-system wait --for=create configmap/local-path-config --timeout=300s
current=$(k -n kube-system get cm local-path-config -o jsonpath='{.data.config\.json}' | tr -d '[:space:]')
if [[ $current != "$desired" ]]; then
    [[ $current == "$default" ]] || fail 'Existing provisioner mapping differs; refusing to overwrite it'
    claims=$(k get pvc -A -o jsonpath='{range .items[?(@.spec.storageClassName=="local-path")]}{.metadata.namespace}{"/"}{.metadata.name}{"\n"}{end}')
    [[ -z $claims ]] || fail 'Existing local-path PVCs require operator review before changing provisioner paths'
fi
# Keep the already-installed bundled deployment/RBAC/StorageClass, and prevent
# K3s from resetting its ConfigMap to a single default path on the next restart.
install -d /var/lib/rancher/k3s/server/manifests
touch /var/lib/rancher/k3s/server/manifests/local-storage.yaml.skip
if [[ $current != "$desired" ]]; then
    patch=$(printf '{"data":{"config.json":"%s"}}' "${desired//\"/\\\"}")
    k -n kube-system patch cm local-path-config --type=merge -p "$patch"
    # Restart to consume the new ConfigMap immediately rather than race reload.
    k -n kube-system rollout restart deployment/local-path-provisioner
    k -n kube-system rollout status deployment/local-path-provisioner --timeout=300s
fi
printf 'PASS: bundled local-path provisioner uses the four inventoried filesystems.\n'
REMOTE
}

verify_storage_config() {
    on_node 1 "$(storage_config_json)" <<'REMOTE'
[[ -f /var/lib/rancher/k3s/server/manifests/local-storage.yaml.skip ]] || fail 'Storage configuration persistence guard is missing; rerun the server script'
current=$(k -n kube-system get cm local-path-config -o jsonpath='{.data.config\.json}' | tr -d '[:space:]')
[[ $current == "$1" ]] || fail 'Provisioner paths differ from inventory; review configuration before creating PVCs'
actual=$(k get storageclass local-path -o jsonpath='{.provisioner}{"|"}{.volumeBindingMode}')
[[ $actual == 'rancher.io/local-path|WaitForFirstConsumer' ]] || fail 'Unexpected local-path StorageClass configuration'
REMOTE
}

storage_test() {
    local target
    for target in 1 2 3 4; do
        on_node 1 "$target" "$verification_image" "${config[NODE${target}_STORAGE_PATH]%/}/vietpulse/local-path" <<'REMOTE'
target=$1
image=$2
expected_path=$3
claim="vp-foundation-storage-$target"
existing=$(k -n kube-system get pvc "$claim" --ignore-not-found -o jsonpath='{.metadata.labels.vietpulse\.io/probe}{"|"}{.spec.storageClassName}')
if [[ -n $existing ]]; then
    [[ $existing == 'k3s-foundation|local-path' ]] || fail 'Storage probe PVC name is occupied by an unrelated claim'
else
    k create -f - <<JSON
{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":"$claim","namespace":"kube-system","labels":{"vietpulse.io/probe":"k3s-foundation"}},"spec":{"accessModes":["ReadWriteOnce"],"storageClassName":"local-path","resources":{"requests":{"storage":"16Mi"}}}}
JSON
fi
pod="vp-foundation-disk-$probe_id-$target"
create_probe_pod "$pod" "$target" "$image" "$claim"
# WaitForFirstConsumer: create a schedulable pod before waiting for the PVC.
k -n kube-system wait --for=jsonpath='{.status.phase}'=Bound "pvc/$claim" --timeout=300s
volume=$(k -n kube-system get pvc "$claim" -o jsonpath='{.spec.volumeName}')
actual_path=$(k get pv "$volume" -o jsonpath='{.spec.local.path}')
[[ $actual_path == "$expected_path/"* ]] || fail 'Probe PV is outside the inventoried storage path'
marker="vietpulse-$probe_id-$target"
# The container shell expands its positional argument, not the operator shell.
# shellcheck disable=SC2016
k -n kube-system exec "$pod" -- sh -ec 'printf %s "$1" > /data/vp-foundation-marker' sh "$marker"
k -n kube-system delete pod "$pod" --wait=true --timeout=60s
pod="vp-foundation-read-$probe_id-$target"
create_probe_pod "$pod" "$target" "$image" "$claim"
actual=$(k -n kube-system exec "$pod" -- cat /data/vp-foundation-marker)
[[ $actual == "$marker" ]] || fail 'PVC contents did not survive pod recreation on the same node'
printf 'PASS: Node %s PVC bound, written and read after pod recreation; retaining %s.\n' "$target" "$claim"
REMOTE
    done
}
