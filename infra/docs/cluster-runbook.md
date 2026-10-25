# Four-node deployment configuration and preflight

Task 1 defines the inventory and software target. It does not install K3s or
prepare Ubuntu. Run preflight only against the four explicitly configured hosts.

## Node inventory

Use the contiguous names below throughout deployment. Older numbering in the
project PRD is historical; it must not be copied into new commands or manifests.

| Node | Required hostname | K3s role | Initial application placement |
| --- | --- | --- | --- |
| 1 | `vp-node-1` | Server | API, dashboards, monitoring, operators |
| 2 | `vp-node-2` | Agent | Kafka, HDFS DataNode, batch Spark |
| 3 | `vp-node-3` | Agent | Kafka, crawlers, streaming Spark |
| 4 | `vp-node-4` | Agent | Kafka, HDFS NameNode/DataNode, catalog, databases, Spark executors |

In particular, the HDFS DataNodes belong on Nodes **2 and 4**. All four nodes
must reach one another; Node 1 needs a stable LAN address. Stable addresses on
Nodes 2-4 are also preferred. GPU enablement is outside this baseline.

Real IPs, Linux usernames, network interfaces, mount paths, filesystem capacity,
and Ethernet/Wi-Fi details are not supplied in the repository. Record observed
values rather than inferring them from hardware advertisements or old numbering.
Do not resize, format, mount, or repartition disks as part of preflight.

## Operator preparation

1. Boot all four machines into Ubuntu 22.04 or 24.04 LTS on the same trusted LAN.
   Prefer 24.04 for new installations; preflight does not install or upgrade it.
2. Prepare unique hostnames `vp-node-1` through `vp-node-4` and working SSH
   authentication. The operator may run from Node 1 or another Linux machine;
   all four hosts, including Node 1, are inspected over SSH.
3. Verify each SSH host-key fingerprint through a trusted channel, then enroll
   it manually in the operator's default `known_hosts`. Preflight requires
   existing trusted keys and never accepts unknown keys or passwords.
4. Ensure the operator has Bash 4.4+, OpenSSH client, and GNU `timeout`. Hosts
   need Bash, `awk`, `lscpu`, `free`, `df`, `findmnt`, `ip`, `ping`, `sudo`, UFW,
   and GNU `timeout`. Ubuntu preparation is a separate human prerequisite.
5. Arrange an existing, noninteractive permission to run
   `sudo -n ufw status verbose` on each host. Preflight does not edit sudoers.
   It fails if that read cannot be performed; use only the privileges needed.
6. Identify the existing Linux filesystem intended to hold persistent data on
   each node, using `lsblk -f`, `findmnt`, and `df -h`. Supply an existing directory
   on that filesystem as `NODE<n>_STORAGE_PATH`. This is the filesystem to measure,
   not permission to create a directory or configure K3s's local-path provisioner.
   Task 2 must align the actual provisioner path with the verified filesystem.

Create the local inventory from the repository root:

```bash
cp infra/.env.example infra/.env
chmod 600 infra/.env
```

Fill every blank `NODE1_*` through `NODE4_*` and `FIREWALL_POLICY` with actual
inputs. See [the environment configuration guide](env-configuration.md) for
field-by-field instructions, collection commands, and a complete example.
IPs must be distinct RFC 1918 IPv4 addresses; DNS names, loopback, and
documentation addresses are rejected. Usernames use lowercase letters, digits,
underscores and hyphens, starting with a letter or underscore. Storage paths must
be absolute and contain only letters, digits, `/`, `.`, `_`, and `-`.

The file accepts literal `KEY=value` lines, blank lines, and full-line `#`
comments. It is **not sourced**: quotes, expansions, `export`, inline comments,
unknown keys and duplicate keys are not supported. Environment variables do not
override inventory entries. Optional `SSH_PORT` defaults to 22;
`SSH_IDENTITY_FILE` may point to an existing absolute private-key path outside
the repository, or authentication can use an existing SSH agent/default key.
SSH config files, local hooks, forwarding, and shared sessions are bypassed.

## Explicit firewall policy

Choose exactly one policy in `infra/.env`; there is no default:

| Value | Required observed state | Operator responsibility |
| --- | --- | --- |
| `ufw-rules` | UFW active | Review LAN-restricted K3s rules, pod/service routing and forwarding |
| `ufw-disabled` | UFW inactive | Confirm the trusted LAN and any other host/router firewall rules |

Preflight prints `ufw status verbose` and fails on a mismatch. It never enables,
disables, or changes a firewall, router, route, interface, or hostname.
An inactive UFW is not proof that nftables, router ACLs, or other firewalls allow
traffic. For default Flannel VXLAN, review:

| Traffic | Direction |
| --- | --- |
| SSH TCP, configured port | Operator to Nodes 1-4 |
| TCP 6443 | Nodes 2-4 to Node 1 |
| TCP 10250 | All nodes to all nodes |
| UDP 8472 | All nodes to all nodes, trusted LAN only |

Review pod/service CIDRs and forwarding as described in the
[K3s requirements](https://docs.k3s.io/installation/requirements), including
overlap with the actual LAN/VPN. Never expose VXLAN to the public Internet.

## Run and interpret preflight

```bash
bash infra/scripts/00-preflight.sh --env-file infra/.env --read-only
```

All inputs are validated before SSH. Each node then reports its hostname, OS,
CPU topology, RAM in MiB, backing filesystem, total/used/available space in KiB,
network interfaces, routes, peer reachability and firewall policy. Peer checks
cover all 12 directed node pairs; ICMP must be permitted for this gate.
Each SSH session is bounded to 60 seconds and each filesystem probe to 5 seconds.

Exit 0 means that all four inventories were collected, expected hostnames/Ubuntu
releases matched, peers answered ping, and the chosen UFW state matched. Any
invalid input, missing tool, SSH failure, hostname/OS mismatch, missing path,
failed ping, unreadable firewall, or policy mismatch stops the script with a
nonzero exit code before proceeding to another node. Correct the reported issue
manually, then rerun the same command.

K3s ports are reported as `UNVERIFIED`: services may not be listening yet, and a
UDP probe cannot establish end-to-end VXLAN health. UFW rules require manual
review here; Task 2 must verify actual cross-node networking and DNS after
installation. Inventory success does not certify disk speed, storage durability,
or sufficient resources for all workloads. Compare measured free space against
the PRD's storage budget before creating PVCs, especially on dual-boot machines.

To retain a report locally, first create `infra/reports/` on the **operator**
machine, then run with pipe failure detection:

```bash
set -o pipefail
bash infra/scripts/00-preflight.sh --env-file infra/.env --read-only \
  2>&1 | tee infra/reports/preflight.txt
```

Local inventory, reports, kubeconfigs, private keys and generated Secrets are
ignored by Git. Keep join tokens/passwords out of this file: preflight rejects
unknown keys without echoing their values. Provision runtime Secrets in later
tasks; never commit credentials or invent application image references.

## Version selection and compatibility gates

[`../versions.yaml`](../versions.yaml) is the explicit deployment target,
reviewed on 2026-10-25. These are upstream-supported combinations selected for
reproducibility, not a benchmark result or proof that the whole stack is deployed.
Later scripts/manifests must consume the pins, render charts for Kubernetes
1.35, and pass each task's live smoke checks before dependent services run.

- K3s `v1.35.9+k3s1` uses Kubernetes 1.35.9; match `kubectl` to it. Retain K3s's
  bundled networking, ingress, DNS and storage versions. Helm 3.20 uses the
  Kubernetes 1.35 client; pin its patch 3.20.2 rather than switching Helm major.
  Sources: [K3s release bundle](https://docs.k3s.io/release-notes/v1.35.X),
  [Helm 3.20 client](https://github.com/helm/helm/releases/tag/v3.20.0),
  [Helm patch](https://github.com/helm/helm/releases/tag/v3.20.2).
- Strimzi 1.2.0 supports Kafka 4.2.1 and tests Kubernetes 1.30-1.36.
  Source: [Strimzi version matrix](https://strimzi.io/downloads/).
- Spark Operator 2.5.2 uses Spark 4.0.4. Choose that Spark release with Scala
  2.13, Java 17, Python 3.11, and its matching Kafka connector. Spark 4.0.4
  supports Hive Metastore 4.0.1; Hive documents that release with Hadoop 3.3.6.
  Use a separate supported JVM/image for Hadoop/Hive rather than assuming that
  every component shares Spark's Java runtime. Sources:
  [operator release](https://github.com/kubeflow/spark-operator/releases/tag/v2.5.2),
  [Spark runtime](https://spark.apache.org/docs/4.0.4/),
  [metastore compatibility](https://spark.apache.org/docs/4.0.4/sql-data-sources-hive-tables.html),
  [Hive release pairing](https://hive.apache.org/general/downloads/).
- **Application gate for Task 6:** the existing development image and
  `pyproject.toml` still use Spark 3.5.3 and Scala 2.12 connectors. The target above
  requires an explicit application migration, dependency/image alignment,
  connector replacement, and streaming/catalog regression tests. Task 1 does
  not perform that migration. Do not deploy the current development image as
  though it already implements this target. Task 4 must test metastore schema
  creation against PostgreSQL 16.15, with matching JDBC/client jars; Task 6 must
  set `spark.sql.hive.metastore.version=4.0.1` and package compatible client jars.
  Source: [PostgreSQL release](https://www.postgresql.org/docs/release/16.15/).
- MongoDB Controllers for Kubernetes (MCK) replaces the archived Community
  Operator. Use its Community API with one replica-set member on Node 4, without
  Ops Manager/cloud dependencies. Pin MongoDB 7.0.45 within the repo's existing
  major line. Sources: [old operator notice](https://github.com/mongodb/mongodb-kubernetes-operator),
  [MCK releases](https://github.com/mongodb/mongodb-kubernetes/releases),
  [MongoDB patches](https://www.mongodb.com/docs/manual/release-notes/7.0/).
- ECK 3.5.0 supports Kubernetes 1.35 and Elastic Stack 8.x. Keep Elasticsearch
  and Kibana on the same 8.19.23 patch. Sources:
  [ECK compatibility](https://www.elastic.co/docs/deploy-manage/deploy/cloud-on-k8s),
  [ECK releases](https://github.com/elastic/cloud-on-k8s/releases),
  [Elastic patch notes](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/release-notes-8.19.23.html).
- Pin the kube-prometheus-stack and Loki charts, including their default image
  versions; tune single-node placement, retention and resource limits in Task 7.
  Loki should use monolithic deployment and local persistence for this demo.
  Sources: [Prometheus chart](https://github.com/prometheus-community/helm-charts/tree/kube-prometheus-stack-92.13.1/charts/kube-prometheus-stack),
  [Loki chart](https://github.com/grafana/loki/tree/helm-loki-7.5.0/production/helm/loki).

If a later task finds an unsupported combination or missing image artifact, stop
that deployment step and revise the pins with evidence. Never silently fall back
to `latest`, substitute an untested major version, or bypass an application gate.

## Local verification

```bash
bash -n infra/scripts/00-preflight.sh
python -m pytest tests/test_preflight.py -q
```

The tests run the real remote probe body through fake SSH and operating-system
commands. They need no live cluster, SSH credentials, sudo access, or root.
Live preflight remains pending until the operator supplies the actual inventory
and prepares access to all four hosts.

## Task 2: Bootstrap the K3s foundation

The scripts are implemented and tested locally. Live installation and acceptance
remain pending until the operator fills `infra/.env`, finishes Ubuntu preparation,
and runs the commands below against the real machines.

### Additional preparation

Complete the read-only preflight first. On each explicitly selected installation
host, disable swap as part of manual Ubuntu preparation, verify the inventory IP
is assigned to the host, and install `curl`. The scripts also use Ubuntu's Bash,
systemd, UFW, `ip`, `swapon`, `findmnt`, `df`, `awk`, and coreutils. Review LAN/VPN
overlap with K3s's default pod/service CIDRs (`10.42.0.0/16`, `10.43.0.0/16`) and
the firewall rules above. Hosts need outbound HTTPS access to download K3s and
pull its bundled images and the pinned verification image.

Task 2 executes privileged programs using `sudo -n bash -s` over SSH. Have the
administrator arrange that noninteractive access; Task 1's permission to read
UFW status alone is insufficient. Node 1 also needs this access for subsequent
cluster administration. The scripts do not edit sudoers, firewall rules, swap,
hostnames, partitions, mounts, or router settings.

Run from the repository root on the operator machine. The default inventory is
`infra/.env`; every script also accepts `--env-file /absolute/path/to/inventory`.
Kubernetes commands use Node 1's bundled `k3s kubectl` and root-only kubeconfig
over SSH, so a separate local kubectl or Helm installation is unnecessary here.

### Preview and install the server

After filling the inventory, preview each operation with `--dry-run`. Dry runs
validate local inputs and pins, then print the plan without SSH, sudo, token
prompts, downloads, directory creation, or Kubernetes mutations.

```bash
bash infra/scripts/00-preflight.sh --env-file infra/.env --read-only
bash infra/scripts/01-install-k3s-server.sh --node 1 --dry-run
bash infra/scripts/02-join-k3s-agent.sh --node 2 --dry-run
bash infra/scripts/verify-cluster.sh --check-network --check-storage --dry-run

bash infra/scripts/01-install-k3s-server.sh --node 1
```

Only `--node 1` is accepted by the server installer. It pins the release from
`infra/versions.yaml`, keeps bundled Traefik, CoreDNS, metrics-server and local-path
storage, and waits for Node 1 and the system deployments. No agent is installed
by this command. Each installation checks the selected host's hostname, Ubuntu
release, IP, swap, storage directory and observed firewall state before installing.

### Supply the token and join one agent at a time

The join command reads `K3S_TOKEN` from its environment, or prompts without echo
when it is absent. Never put it in the inventory, a command argument, a committed
file, or a pasted command containing the literal token. For example, capture it
directly into an operator shell variable, replacing the SSH destination with
Node 1's real username and IP:

```bash
set +xv
K3S_TOKEN=$(ssh -F /dev/null -T -o BatchMode=yes -o StrictHostKeyChecking=yes \
  -o UpdateHostKeys=no <NODE1_USER>@<NODE1_IP> \
  sudo -n cat /var/lib/rancher/k3s/server/node-token)
export K3S_TOKEN
```

Use the inventory's port (`-p`) and identity (`-i`, `-o IdentitiesOnly=yes`) when
needed. That capture command requires permission to read the root-owned token.
Keep the token in the operator's session only for the joins:

```bash
bash infra/scripts/02-join-k3s-agent.sh --node 2
bash infra/scripts/02-join-k3s-agent.sh --node 3
bash infra/scripts/02-join-k3s-agent.sh --node 4
unset K3S_TOKEN

bash infra/scripts/03-label-nodes.sh
bash infra/scripts/04-create-namespaces.sh
bash infra/scripts/verify-cluster.sh --check-network --check-storage
```

Run these commands sequentially and stop if any command fails. Each join requires
exactly one `--node` from 2–4 and verifies the server first. Node 2 must run a real
probe pod, resolve cluster DNS and exchange HTTP traffic with a Node 1 pod.
Joining Node 3 or 4 repeats that two-node gate before touching the selected host.
The token travels over encrypted SSH stdin and is stored only in the agent's
root-only `/etc/rancher/k3s/vietpulse-agent-token`; its configuration references
that file. Shell tracing is disabled before processing tokens, including dry runs.

### Storage configuration and verification

The server maps each `vp-node-N` to a dedicated `vietpulse/local-path` child under
its inventoried `NODE<N>_STORAGE_PATH`. Provisioning creates directories there;
no disks are formatted or mounted. It patches only `config.json` in the bundled
`kube-system/local-path-config` ConfigMap. Before changing an initial default
mapping, it refuses existing local-path claims or an unexpected mapping.

After the bundled storage components are installed, the server creates
`/var/lib/rancher/k3s/server/manifests/local-storage.yaml.skip`. This preserves the
installed components while preventing K3s from resetting the per-node ConfigMap
on restart. The provisioner is restarted after the first mapping change to load
it immediately. Keep this guard when restarting the pinned release; review the
storage bundle explicitly during a future K3s upgrade. See K3s's
[packaged-component lifecycle](https://docs.k3s.io/installation/packaged-components)
and the provisioner's [nodePathMap configuration](https://github.com/rancher/local-path-provisioner#configuration).

The verifier always checks exactly four Ready nodes with the expected names,
InternalIPs and pinned kubelet versions, the system deployments, required role
labels, six Active namespaces, StorageClass and per-node storage mapping.
`--check-network` additionally resolves `kubernetes.default.svc.cluster.local`
from every node and checks all 12 directed cross-node HTTP paths using pod IPs.
`--check-storage` creates or reuses one labelled 16Mi probe claim per node:

```text
kube-system/vp-foundation-storage-1
kube-system/vp-foundation-storage-2
kube-system/vp-foundation-storage-3
kube-system/vp-foundation-storage-4
```

Each storage check creates a schedulable consumer before waiting for the
`WaitForFirstConsumer` PVC to bind, checks the actual PV directory, writes a fresh
marker, deletes its pod, then reads the marker from a replacement pod pinned to
the same node. Probe pods have resource limits and a ten-minute active deadline;
the scripts clean up their own pods on success or ordinary failure. Test claims
are retained for reruns and inspection. PVC/PV deletion remains an explicit
operator action requiring the approval described in PRD section 18. Local-path
storage is node-local; this check does not demonstrate cross-node data failover.

### Failure handling and reruns

Matching active installations are reused without rerunning the installer.
Conflicting versions/configurations, unmanaged installations and partial installs
stop for inspection. The scripts never upgrade, uninstall, reset a cluster, change
an existing agent token, or remove persistent data automatically. Labels and
namespace creation can be rerun; storage verification reuses only its labelled
claims. Run one bootstrap/verification operation at a time.

SSH operations have a 20-minute outer deadline. Kubernetes creation/readiness
waits and deployment rollouts have five-minute deadlines; HTTP probes allow ten
seconds per peer. Errors preserve a nonzero exit status and report the failing
node and remote line or the failed Kubernetes operation. Fix the reported issue
before rerunning the same step. An interrupted installer may leave a partial
installation requiring manual review.

For diagnosis, inspect `sudo systemctl status k3s --no-pager` on Node 1 or
`sudo systemctl status k3s-agent --no-pager` on the selected agent. On Node 1,
use `sudo k3s kubectl get pods -A -o wide`, `sudo k3s kubectl get events -A`, and
`sudo k3s kubectl describe pod -n kube-system <PROBE_POD>` to investigate image
pulls, scheduling, DNS or PVC binding. Inspect retained claims with
`sudo k3s kubectl get pvc -n kube-system`.

Local checks, requiring no real `.env`, root privileges or SSH credentials:

```bash
for script in infra/scripts/*.sh infra/scripts/lib/*.sh; do bash -n "$script"; done
shellcheck -x -P SCRIPTDIR infra/scripts/*.sh infra/scripts/lib/*.sh
python -m pytest tests/test_preflight.py tests/test_k3s_foundation.py -q
```

## Kafka KRaft (Task 3)

The `multi-node` overlay deploys Kafka **4.2.1** using Strimzi **1.2.0** into
`messaging`. It contains one Kafka resource, three single-instance node pools,
and the six required project topics. Helm values are separate from the Kustomize
resources. The Cluster Operator and Topic Operator select Node 1's control role;
the broker/controllers select these roles:

| Pool | Required labels | Expected node | Local-path PVC | Memory request / limit |
| --- | --- | --- | --- | --- |
| `batch` | `kafka=true`, `batch=true` | 2 | 40Gi | 2Gi / 2560Mi |
| `streaming` | `kafka=true`, `streaming=true` | 3 | 25Gi | 2Gi / 2560Mi |
| `data` | `kafka=true`, `data=true` | 4 | 40Gi | 2Gi / 3Gi |

All role keys have the `vietpulse.io/` prefix. Each broker/controller has a 1Gi
Java heap; its memory limit also allows space for native memory and page cache.
PVCs have `deleteClaim: false`. Do not rename the Kafka cluster or node pools,
change their roles, recreate topics, prune resources, or delete PVCs to redeploy.
These operations are not part of this task's rerun procedure.

Clients inside Kubernetes use
`vietpulse-kafka-bootstrap.messaging.svc:9092`. The listener uses plaintext with
no authentication inside the trusted demo cluster; no external listener is
configured. Topic auto-creation is disabled. All six project topics use
replication factor 3 and minimum ISR 2; producers requiring that durability must
use `acks=all`. Partitions and retention match `docker/kafka/create-topics.sh`.
The optional YouTube and Compose-only `dev.smoke` topics are not deployed.

### Local checks before the workers are available

Install the existing development dependencies, including PyYAML for manifest
checks, in a virtual environment:

```bash
python -m pip install -e '.[crawler,dev]'
python -m pytest tests/test_preflight.py tests/test_k3s_foundation.py tests/test_kafka_deployment.py -q
ruff check .
ruff format --check .
for script in infra/scripts/*.sh infra/scripts/lib/*.sh; do bash -n "$script"; done
shellcheck -x -P SCRIPTDIR infra/scripts/*.sh infra/scripts/lib/*.sh
kubectl kustomize k8s/overlays/multi-node
helm template strimzi strimzi-kafka-operator --repo https://strimzi.io/charts/ --version 1.2.0 \
  --namespace messaging --kube-version 1.35.9 -f k8s/base/messaging/operator-values.yaml
```

For a real Kafka test on Node 1 without Docker, root privileges, K3s or workers,
download the official `kafka_2.13-4.2.1.tgz` distribution from
[Apache Kafka](https://downloads.apache.org/kafka/4.2.1/) and verify its published
SHA-512 checksum before extracting it. With Java 17+ and the development virtual
environment active, run:

```bash
python tests/integration/kafka_local_smoke.py --kafka-home /absolute/path/to/kafka_2.13-4.2.1
```

This starts three broker/controller processes on randomly selected loopback
ports, creates the topics directly from the deployment manifests, verifies the
live partition/retention/replication settings, and produces/consumes unique
markers in an isolated probe topic. It stops one broker, verifies writes with
`acks=all` still work, restarts that broker using the same data directory, and
repeats topic creation. Existing topic IDs and both markers must survive. The
test stops its own processes on exit and retains its unique temporary log/data
directory for diagnosis. It needs about 2-3GiB available memory.

Passing these local checks does **not** demonstrate Strimzi reconciliation,
cross-node networking, Kubernetes scheduling, or PVC persistence. Task 3's live
acceptance checks remain pending until all four nodes are available and Task 2
has passed. Do not weaken the multi-node affinity or replication settings to
make that verification pass on one machine.

### Deploy after Task 2 passes

Run the existing foundation check first from the repository root:

```bash
bash infra/scripts/verify-cluster.sh --check-network --check-storage
```

Stop on failure. Before the initial installation, inspect any existing Strimzi
release or `Kafka/vietpulse` resources. Continue only for a new deployment or a
rerun of this same pinned, operator-managed deployment; conflicting existing
state requires inspection rather than overwriting it.

On **Node 1**, with Helm at the version in `infra/versions.yaml` and the repository
checked out, install the pinned operator using the existing root-only kubeconfig:

```bash
source infra/scripts/lib/common.sh
repo_root=$PWD
strimzi_chart=$(version_pin messaging helm_chart)
strimzi_repository=$(version_pin messaging helm_repository)
strimzi_version=$(version_pin messaging chart_version)
sudo helm upgrade --install strimzi "$strimzi_chart" \
  --repo "$strimzi_repository" --version "$strimzi_version" \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --namespace messaging \
  -f k8s/base/messaging/operator-values.yaml --wait --timeout 5m
sudo k3s kubectl wait --for=condition=Established --timeout=60s \
  crd/kafkas.kafka.strimzi.io crd/kafkanodepools.kafka.strimzi.io crd/kafkatopics.kafka.strimzi.io
sudo k3s kubectl apply -k k8s/overlays/multi-node
bash infra/scripts/verify-kafka.sh
```

The verifier uses the same literal `infra/.env`, strict SSH options and Node 1
administration as the foundation scripts; `--env-file FILE` and `--dry-run` are
supported. A dry run validates configuration without connecting to any host.
Live verification rejects missing workers, wrong versions, stale Ready status,
wrong node placement, pending/wrong-sized PVCs, drifted topic settings, missing
partitions and under-replicated partitions. Waits are bounded and failures exit
nonzero. A snapshot check is not continuous health monitoring.

Run deployment/verification operations **one at a time**. The verifier creates
only its labelled `messaging/vietpulse.infra.verify` KafkaTopic and
`messaging/vp-kafka-verification` ConfigMap. It checks resource ownership before
writing, uses one compacted key to retain the latest successful marker, and
does not send synthetic records into the project topics. Later runs read that
previous marker before producing a fresh UUID. They fail if it is missing or
changed. These probe resources are deliberately retained; do not delete them
between a deployment and its preservation check.

To verify repeated deployment, run the verifier successfully once, then on Node 1:

```bash
sudo k3s kubectl apply -k k8s/overlays/multi-node
bash infra/scripts/verify-kafka.sh
```

The second verification must confirm the previous marker survived and produce
and consume a new marker. It never deletes topics, pods, PVCs or PVs. Broker
restart tests on the real cluster belong to the later acceptance suite.

References: [Strimzi node pools and storage](https://strimzi.io/docs/operators/1.2.0/deploying.html#assembly-node-pools-str),
[Strimzi topic management](https://strimzi.io/docs/operators/1.2.0/deploying.html#using-the-topic-operator-str),
and [Kafka KRaft initialization](https://kafka.apache.org/42/operations/kraft/).
