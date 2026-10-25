# Configure the deployment inventory in infra/.env

This guide explains how to collect and fill the inputs for the four-node
read-only preflight. Run commands marked **on each node** on the corresponding
Ubuntu machine; run commands marked **on the operator machine** on the Linux
machine from which you will administer the cluster.

Maintain one inventory on the operator machine containing all four nodes.
The operator may be Node 1 or another Linux machine. Even when running from
Node 1, preflight connects to Node 1 over SSH as well as Nodes 2-4.

## 1. Create the local file

On the operator machine, from the repository root:

```bash
cp -n infra/.env.example infra/.env
chmod 600 infra/.env
```

The copy command preserves an existing `infra/.env`. Open that file in your
editor and fill the required blanks. Leave `infra/.env.example` as the shared
template. Git ignores `infra/.env`.

Use literal assignments with no spaces around `=`:

```dotenv
# A full-line comment is allowed.
NODE1_IP=172.16.50.11
NODE1_USER=alice
```

Do not use quotes, `export`, variable expansion, commands, or inline comments.
For example, write an absolute path instead of `~/.ssh/id_ed25519` or
`$HOME/.ssh/id_ed25519`. Do not run `source infra/.env`; preflight reads it as
data. Each key must appear exactly once.

## 2. Confirm the node mapping

| Inventory prefix | Required hostname | Role |
| --- | --- | --- |
| `NODE1_` | `vp-node-1` | K3s server/control plane |
| `NODE2_` | `vp-node-2` | K3s agent |
| `NODE3_` | `vp-node-3` | K3s agent |
| `NODE4_` | `vp-node-4` | K3s agent |

On each node, check its current hostname:

```bash
hostname
```

The inventory does not rename hosts. If a hostname differs from the expected
name, confirm that you are configuring the correct machine and complete the
hostname preparation described in the [cluster runbook](cluster-runbook.md).

## 3. Collect the required values on each node

Repeat the following checks on Nodes 1, 2, 3, and 4 and record the results in
the matching `NODE<n>_` entries. The four nodes can have different usernames and
storage paths.

### `NODE<n>_IP`: the node's LAN IPv4 address

On each node:

```bash
ip -4 -br address
```

Choose the IPv4 address on the Ethernet or Wi-Fi interface used to communicate
with the other nodes. If the output contains `172.16.50.11/24`, enter only
`172.16.50.11`, without `/24`. Do not choose loopback, Docker/Podman bridges, or
an unrelated VPN interface.

All four IPs must be different. Preflight accepts private IPv4 addresses in
`10.0.0.0/8`, `172.16.0.0/12`, or `192.168.0.0/16`. Use ordinary decimal octets
without leading zeros. Hostnames, `localhost`, `127.0.0.1`, public IPs, IPv6,
and documentation addresses such as `192.0.2.10` are not accepted.

Use the address actually assigned to each machine. Keep Node 1's address stable
and update the inventory if any node's DHCP address changes.

### `NODE<n>_USER`: the SSH login account on that node

On each node, while logged in as the account you intend to use for SSH:

```bash
whoami
```

Copy that Linux username. It is not the hostname, your full display name, or
the operator machine's username unless the accounts happen to match.
Preflight accepts a lowercase letter or underscore followed by lowercase
letters, digits, underscores, or hyphens.

The account needs working SSH key authentication and noninteractive permission
for the read-only command `sudo -n ufw status verbose`. A successful interactive
login alone does not establish those prerequisites; see step 6.

### `NODE<n>_STORAGE_PATH`: an existing directory on the intended data filesystem

On each node, inspect the Linux filesystems:

```bash
lsblk -f
findmnt
df -h
```

Choose an existing directory on the filesystem intended to hold the node's
persistent data. To inspect a candidate such as `/`, run:

```bash
findmnt --target / --output SOURCE,FSTYPE,TARGET
df -h -- /
```

Replace `/` in those inspection commands with your actual candidate directory
when using a separate data filesystem. Check its mounted filesystem and
available capacity, rather than the drive's advertised size, Windows free
space, or unallocated disk space.

The value must be an absolute directory path using only letters, digits, `/`,
`.`, `_`, and `-`. Do not enter a block device such as `/dev/nvme0n1p3`, a
relative path, or a directory that does not yet exist.

`/` is valid only when you intend to use the root filesystem and its measured
free space meets the deployment budget. If you intend to use a separate mounted
data filesystem, select a directory on that filesystem instead.

This value tells preflight which filesystem to measure. It does not create a
directory, mount a disk, reserve capacity, or configure persistent volumes.
Task 2 must align the actual K3s storage location with the verified filesystem.

## 4. Choose FIREWALL_POLICY explicitly

On each node, inspect the current state:

```bash
sudo ufw status verbose
```

| Inventory value | Expected output on all four nodes | Meaning |
| --- | --- | --- |
| `ufw-rules` | `Status: active` | Keep UFW active and review the required LAN rules |
| `ufw-disabled` | `Status: inactive` | UFW is disabled for the trusted-LAN demo |

There is one shared policy for the four nodes. Choose the team's intended
policy; do not select a value merely to suppress a mismatch. If the nodes have
different UFW states, arrange the chosen policy manually before running the
preflight. Editing this variable never changes a firewall.

For `ufw-rules`, the state check does not validate every rule. Review the K3s
traffic directions and forwarding requirements in the
[runbook's firewall section](cluster-runbook.md#explicit-firewall-policy).

## 5. Fill the optional SSH settings on the operator machine

| Key | What to enter |
| --- | --- |
| `SSH_PORT` | A port from 1 to 65535; leave `22` for standard SSH |
| `SSH_IDENTITY_FILE` | An absolute path to a readable private key on the operator machine, or leave empty |

Both settings apply to all four SSH connections. The current script requires
one shared SSH port; it does not support a different port for each node.

If you use a dedicated identity, an example assignment is
`SSH_IDENTITY_FILE=/home/operator/.ssh/id_ed25519`. Replace the path with the
actual file on the operator machine. Enter only its path, never the key contents
and never the `.pub` file. Keep the key outside the repository.

If this setting is empty, authentication may use an existing SSH agent or
OpenSSH's default identity files. Preflight ignores SSH configuration files, so
custom `Host` aliases, ports, and `IdentityFile` settings in `~/.ssh/config` do
not apply. Pass a nondefault port or dedicated key through the inventory.

## 6. Confirm SSH access and read-only firewall permission

Before preflight, follow the
[operator preparation instructions](cluster-runbook.md#operator-preparation)
to verify and enroll all four host keys in the operator's `known_hosts`.

For a manual check from the operator machine, replace the example username,
IP, and port below with actual values, then repeat for all four nodes:

```bash
ssh -F /dev/null -o BatchMode=yes -o StrictHostKeyChecking=yes \
  -o UpdateHostKeys=no -p 22 alice@172.16.50.11 hostname
```

If `SSH_IDENTITY_FILE` is set, add `-i /absolute/path/to/private-key` and
`-o IdentitiesOnly=yes` before the `user@IP` destination in the manual command.

Using the same SSH options and destination, replace the final `hostname` with
`sudo -n ufw status verbose` to check the firewall read permission. That command
must succeed without asking for a password. If it fails, have the administrator
arrange permission for that read-only command. A password cached by a previous
interactive sudo session should not be treated as a permanent prerequisite.

Do not put SSH passwords, sudo passwords, K3s join tokens, or application
credentials into this inventory; those are not accepted configuration keys.

## 7. Review a complete example

The following values are fictional. They demonstrate the format and must be
replaced with actual observations. In particular, the root filesystem paths
below are examples, not a storage recommendation for your machines.

```dotenv
NODE1_IP=172.16.50.11
NODE1_USER=alice
NODE1_STORAGE_PATH=/
NODE2_IP=172.16.50.12
NODE2_USER=bob
NODE2_STORAGE_PATH=/
NODE3_IP=172.16.50.13
NODE3_USER=carol
NODE3_STORAGE_PATH=/
NODE4_IP=172.16.50.14
NODE4_USER=dave
NODE4_STORAGE_PATH=/

FIREWALL_POLICY=ufw-rules
SSH_PORT=22
SSH_IDENTITY_FILE=
```

## 8. Run the read-only preflight

From the repository root on the operator machine:

```bash
bash infra/scripts/00-preflight.sh --env-file infra/.env --read-only
```

Preflight validates the file before connecting, then inspects Nodes 1-4 in
order. An error stops the run. A successful inventory still reports K3s ports
as `UNVERIFIED`; actual cluster networking is checked after installation.

| Message or symptom | What to check |
| --- | --- |
| `NODE<n>_... is required` | Fill the named required entry |
| Invalid assignment, unknown key, or duplicate key | Use only the template keys, each once, with literal `KEY=value` syntax |
| Invalid or duplicate node IP | Use distinct LAN IPv4 addresses without a CIDR suffix |
| SSH host-key verification failure | Verify the host fingerprint and complete trusted-key enrollment |
| SSH permission denied or timeout | Check the IP, login account, key/agent, shared port, and SSH reachability |
| Expected hostname mismatch | Confirm the node mapping and complete hostname preparation |
| Storage path must already exist | Select an existing directory on the intended mounted filesystem |
| Cannot reach peer | Boot all four machines and check LAN routing and ICMP access between them |
| Cannot read firewall status | Check noninteractive permission for `sudo -n ufw status verbose` over SSH |
| Chosen firewall policy mismatch | Compare each node's UFW state with the team's explicit policy |

Return to the [cluster runbook](cluster-runbook.md) for the inventory gate's
limits and the subsequent deployment phases.
