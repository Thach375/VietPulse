"""Local contract checks for the K3s scripts; never contact a real machine."""

import inspect
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "infra/scripts"
COMMANDS = [
    ("01-install-k3s-server.sh", ["--node", "1"]),
    ("02-join-k3s-agent.sh", ["--node", "2"]),
    ("03-label-nodes.sh", []),
    ("04-create-namespaces.sh", []),
    ("verify-cluster.sh", ["--check-network", "--check-storage"]),
]


@pytest.fixture
def operator(tmp_path):
    inventory = tmp_path / "inventory.env"
    values = {"FIREWALL_POLICY": "ufw-disabled"}
    for node in range(1, 5):
        values.update(
            {
                f"NODE{node}_IP": f"192.168.50.{node}",
                f"NODE{node}_USER": "operator",
                f"NODE{node}_STORAGE_PATH": f"/data/node-{node}",
            }
        )
    inventory.write_text("".join(f"{key}={value}\n" for key, value in values.items()))
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    ssh_log = tmp_path / "ssh.jsonl"
    ssh = bin_dir / "ssh"
    ssh.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "with open(os.environ['SSH_LOG'], 'a') as log:\n"
        "    log.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "sys.exit(99)\n"
    )
    ssh.chmod(0o755)

    def run(script, *args, env=None, env_file=inventory):
        result = subprocess.run(
            ["bash", str(SCRIPTS / script), "--env-file", str(env_file), *args],
            env=os.environ
            | {"PATH": f"{bin_dir}:{os.environ['PATH']}", "SSH_LOG": str(ssh_log)}
            | (env or {}),
            capture_output=True,
            text=True,
            timeout=15,
        )
        calls = [json.loads(line) for line in ssh_log.read_text().splitlines()] if ssh_log.exists() else []
        return result, calls

    return run


@pytest.fixture
def cluster(tmp_path):
    """Execute the real remote Bash, translating system paths into a temp tree."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"nodes": {}, "pods": {}, "pvcs": {}, "labels": {}, "namespaces": []}))
    tool = bin_dir / "fake-tool"
    tool.write_text(
        f"#!{sys.executable}\nimport json, os, sys\nfrom pathlib import Path\n"
        + inspect.getsource(fake_host_command)
        + "\nfake_host_command()\n"
    )
    tool.chmod(0o755)
    for command in ("hostname", "systemctl", "curl", "ip", "ufw", "swapon"):
        (bin_dir / command).symlink_to(tool)
    for node in range(1, 5):
        host = tmp_path / f"host-{node}"
        (host / "storage").mkdir(parents=True)
        (host / "os-release").write_text('ID=ubuntu\nVERSION_ID="24.04"\n')
    ssh = bin_dir / "ssh"
    ssh.write_text(
        f"#!{sys.executable}\n"
        "import json, os, pathlib, shlex, subprocess, sys\n"
        "node = sys.argv[-2].split('.')[-1]\n"
        "args = shlex.split(sys.argv[-1])\n"
        "assert args[:5] == ['sudo', '-n', 'bash', '-s', '--']\n"
        "with open(os.environ['SSH_LOG'], 'a') as log:\n"
        "    log.write(json.dumps({'node': node, 'args': sys.argv[1:]}) + '\\n')\n"
        "body = sys.stdin.read()\n"
        "host = pathlib.Path(os.environ['FAKE_ROOT']) / ('host-' + node)\n"
        "replacements = {'/etc/rancher/k3s': str(host / 'config'),\n"
        " '/var/lib/rancher/k3s': str(host / 'data'), '/etc/systemd/system': str(host / 'systemd'),\n"
        " '/usr/local/bin/k3s': str(host / 'k3s'), '/etc/os-release': str(host / 'os-release')}\n"
        "for source, target in replacements.items():\n"
        "    body = body.replace(source, target)\n"
        "    args = [arg.replace(source, target) for arg in args]\n"
        "result = subprocess.run(['bash', '-s', '--', *args[5:]], input=body, text=True,\n"
        " env=dict(os.environ, FAKE_NODE=node, FAKE_HOST=str(host)))\n"
        "sys.exit(result.returncode)\n"
    )
    ssh.chmod(0o755)
    inventory = tmp_path / "inventory.env"
    inventory.write_text(
        "FIREWALL_POLICY=ufw-disabled\n"
        + "".join(
            f"NODE{node}_IP=192.168.50.{node}\nNODE{node}_USER=operator\n"
            f"NODE{node}_STORAGE_PATH={tmp_path}/host-{node}/storage\n"
            for node in range(1, 5)
        )
    )
    ssh_log = tmp_path / "ssh.jsonl"

    def run(script, *args, env=None, trace=False):
        result = subprocess.run(
            ["bash", *(["-xv"] if trace else []), str(SCRIPTS / script), "--env-file", str(inventory), *args],
            env=os.environ
            | {
                "PATH": f"{bin_dir}:{os.environ['PATH']}",
                "SSH_LOG": str(ssh_log),
                "FAKE_ROOT": str(tmp_path),
                "FAKE_STATE": str(state_file),
                "FAKE_TOOL": str(tool),
                "K3S_TOKEN": "K10test::agent:do-not-disclose",
            }
            | (env or {}),
            capture_output=True,
            text=True,
            timeout=30,
        )
        state = json.loads(state_file.read_text())
        calls = [json.loads(line) for line in ssh_log.read_text().splitlines()] if ssh_log.exists() else []
        return result, state, calls

    run.root = tmp_path
    run.state_file = state_file
    return run


def fake_host_command():
    """OS/Kubernetes boundary fake, copied into the temporary executable."""
    node = os.environ["FAKE_NODE"]
    host = Path(os.environ["FAKE_HOST"])
    state_file = Path(os.environ["FAKE_STATE"])
    state = json.loads(state_file.read_text())
    name = Path(sys.argv[0]).name
    args = sys.argv[1:]
    failure = os.environ.get("FAKE_FAILURE", "")

    def save():
        state_file.write_text(json.dumps(state))

    def error(message):
        sys.exit(message)

    if name == "hostname":
        print("vp-node-" + ("wrong" if failure == "hostname" else node))
    elif name == "ip":
        print(f"2: eth0 inet 192.168.50.{node}/24 scope global eth0")
    elif name == "swapon":
        if failure == "swap":
            print("/swapfile file 1024 0 -2")
    elif name == "ufw":
        print("Status: " + ("active" if failure == "firewall" else "inactive"))
    elif name == "systemctl":
        if args[0] == "is-active" and node not in state["nodes"]:
            error("Service is not active")
    elif name == "curl":
        if failure == "download":
            error("Download failed")
        Path(args[args.index("-o") + 1]).write_text('#!/bin/sh\nexec "$FAKE_TOOL" install\n')
    elif args == ["install"]:
        version = os.environ["INSTALL_K3S_VERSION"]
        role = os.environ["INSTALL_K3S_EXEC"]
        (host / "k3s").symlink_to(os.environ["FAKE_TOOL"])
        state["nodes"][node] = version
        state.setdefault("installs", []).append({"node": node, "version": version, "role": role})
        if role == "server":
            state["storage_config"] = {
                "nodePathMap": [
                    {
                        "node": "DEFAULT_PATH_FOR_NON_LISTED_NODES",
                        "paths": [str(host / "storage/vietpulse/local-path")],
                    }
                ]
            }
        save()
    elif args == ["--version"]:
        print("k3s version " + state["nodes"].get(node, "v0.0.0+k3s1"))
    elif args and args[0] == "kubectl":
        args = args[1:]
        args.remove("--kubeconfig")
        args.pop(0)
        args = [arg for arg in args if not arg.startswith("--request-timeout=")]
        if "-n" in args:
            pos = args.index("-n")
            del args[pos : pos + 2]
        verb = args[0]
        if verb == "wait":
            resource = next(
                arg for arg in args if arg.startswith(("node/", "pod/", "pvc/", "configmap/", "deployment/"))
            )
            kind, resource_name = resource.split("/", 1)
            if kind == "node" and resource_name.removeprefix("vp-node-") not in state["nodes"]:
                error("Missing node")
            if kind == "pvc" and not state["pvcs"].get(resource_name, {}).get("bound"):
                error("PVC needs a consumer pod before binding")
        elif verb == "rollout":
            if failure == "system":
                error("System deployment unavailable")
        elif verb == "get":
            kind = args[1]
            query = args[-1]
            if kind == "--raw=/readyz":
                print("ok")
            elif kind == "node":
                target = args[2].removeprefix("vp-node-")
                version = "v0.0.0+k3s1" if failure == "version" else state["nodes"][target]
                print(f"{version}|192.168.50.{target}")
            elif kind == "nodes":
                for target in sorted(state["nodes"]):
                    if "-l" not in args or args[args.index("-l") + 1] in state["labels"].get(target, []):
                        print("node/vp-node-" + target)
            elif kind == "namespace":
                if args[2] not in state["namespaces"]:
                    error("Missing namespace")
                print("Active")
            elif kind in ("cm", "configmap"):
                print(json.dumps(state["storage_config"], separators=(",", ":")))
            elif kind == "storageclass":
                print("rancher.io/local-path|WaitForFirstConsumer")
            elif kind == "pod":
                pod = state["pods"][args[2]]
                target = pod["spec"]["nodeSelector"]["kubernetes.io/hostname"]
                print(target if "nodeName" in query else f"10.42.{target[-1]}.2")
            elif kind == "pvc":
                if "-A" in args:
                    print(" ".join(state["pvcs"]))
                elif args[2] in state["pvcs"]:
                    print("pv-" + args[2] if "volumeName" in query else "k3s-foundation|local-path")
            elif kind == "pv":
                print(state["pvcs"][args[2].removeprefix("pv-")]["path"])
            else:
                error("Unexpected get: " + repr(args))
        elif verb == "create" and args[1] == "-f":
            doc = json.load(sys.stdin)
            resource_name = doc["metadata"]["name"]
            if doc["kind"] == "Pod":
                if resource_name in state["pods"]:
                    error("Pod already exists")
                state["pods"][resource_name] = doc
                for volume in doc["spec"].get("volumes", []):
                    claim = volume["persistentVolumeClaim"]["claimName"]
                    target = doc["spec"]["nodeSelector"]["kubernetes.io/hostname"]
                    entry = next(
                        item for item in state["storage_config"]["nodePathMap"] if item["node"] == target
                    )
                    state["pvcs"][claim].update({"bound": True, "path": entry["paths"][0] + "/pv-" + claim})
            elif doc["kind"] == "PersistentVolumeClaim":
                if resource_name in state["pvcs"]:
                    error("PVC already exists")
                state["pvcs"][resource_name] = {"bound": False}
            save()
        elif verb == "create" and args[1] == "namespace":
            print(json.dumps({"metadata": {"name": args[2]}}))
        elif verb == "apply":
            doc = json.load(sys.stdin)
            if doc["metadata"]["name"] not in state["namespaces"]:
                state["namespaces"].append(doc["metadata"]["name"])
            save()
        elif verb == "patch":
            patch = json.loads(args[args.index("-p") + 1])
            state["storage_config"] = json.loads(patch["data"]["config.json"])
            save()
        elif verb == "label":
            state["labels"][args[2].removeprefix("vp-node-")] = args[3:-1]
            save()
        elif verb == "delete" and args[1] == "pod":
            state["pods"].pop(args[2], None)
            save()
        elif verb == "exec":
            command = args[args.index("--") + 1 :]
            if command[0] == "nslookup":
                if failure == "dns":
                    error("DNS lookup failed")
                pod = state["pods"][args[1]]
                state.setdefault("dns_nodes", []).append(
                    pod["spec"]["nodeSelector"]["kubernetes.io/hostname"]
                )
                save()
            elif command[0] == "wget":
                if failure == "network":
                    error("Cross-node HTTP failed")
                pod = state["pods"][args[1]]
                source = pod["spec"]["nodeSelector"]["kubernetes.io/hostname"][-1]
                state.setdefault("network_pairs", []).append([source, command[-1].split(".")[2]])
                save()
                print("vp-node-" + command[-1].split(".")[2])
            elif command[0] in ("sh", "cat"):
                pod = state["pods"][args[1]]
                claim = pod["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"]
                if command[0] == "sh":
                    state["pvcs"][claim]["contents"] = command[-1]
                    save()
                else:
                    print("corrupt" if failure == "storage" else state["pvcs"][claim]["contents"])
        else:
            error("Unexpected kubectl: " + repr(args))
    else:
        error("Unexpected host command: " + repr((name, args)))


def test_server_installs_only_selected_host_and_persists_storage_map(cluster):
    result, state, calls = cluster("01-install-k3s-server.sh", "--node", "1")
    assert result.returncode == 0, result.stderr
    assert state["installs"] == [{"node": "1", "version": "v1.35.9+k3s1", "role": "server"}]
    assert {call["node"] for call in calls} == {"1"}
    assert len(state["storage_config"]["nodePathMap"]) == 4
    assert (cluster.root / "host-1/data/server/manifests/local-storage.yaml.skip").exists()


def test_agent_failure_gate_prevents_installation_on_later_nodes(cluster):
    result, _, _ = cluster("01-install-k3s-server.sh", "--node", "1")
    assert result.returncode == 0, result.stderr
    result, state, calls = cluster("02-join-k3s-agent.sh", "--node", "3")
    assert result.returncode != 0
    assert set(state["nodes"]) == {"1"}
    assert {call["node"] for call in calls} == {"1"}


def bootstrap(cluster):
    for script, args in [
        ("01-install-k3s-server.sh", ["--node", "1"]),
        *(("02-join-k3s-agent.sh", ["--node", str(node)]) for node in (2, 3, 4)),
        ("03-label-nodes.sh", []),
        ("04-create-namespaces.sh", []),
    ]:
        result, _, _ = cluster(script, *args)
        assert result.returncode == 0, result.stderr


def test_full_foundation_and_repeated_storage_verification(cluster):
    bootstrap(cluster)
    for _ in range(2):
        result, state, calls = cluster("verify-cluster.sh", "--check-network", "--check-storage")
        assert result.returncode == 0, result.stderr
        assert not state["pods"], "Probe pods should be cleaned up"
        assert len(state["pvcs"]) == 4, "Reuse only the four owned test claims"
        assert all(claim["contents"].startswith("vietpulse-") for claim in state["pvcs"].values())
        assert "K10test::agent:do-not-disclose" not in result.stdout + result.stderr + json.dumps(calls)
        assert {tuple(pair) for pair in state["network_pairs"][-12:]} == {
            (str(source), str(target)) for source in range(1, 5) for target in range(1, 5) if source != target
        }
        assert set(state["dns_nodes"][-4:]) == {f"vp-node-{node}" for node in range(1, 5)}
    assert set(state["namespaces"]) == {
        "ingestion",
        "messaging",
        "storage",
        "processing",
        "serving",
        "monitoring",
    }
    assert state["labels"]["4"] == [
        "vietpulse.io/data=true",
        "vietpulse.io/storage=true",
        "vietpulse.io/compute=true",
        "vietpulse.io/kafka=true",
    ]


@pytest.mark.parametrize("failure", ["download", "swap", "firewall", "hostname"])
def test_host_preparation_and_download_failures_stop_server_bootstrap(cluster, failure):
    result, state, _ = cluster("01-install-k3s-server.sh", "--node", "1", env={"FAKE_FAILURE": failure})
    assert result.returncode != 0, result.stdout + result.stderr
    assert not state["nodes"]
    assert not (cluster.root / "host-1/config/config.yaml").exists()


@pytest.mark.parametrize("failure", ["dns", "network"])
def test_failed_node2_probe_prevents_installing_node3(cluster, failure):
    for script, args in COMMANDS[:2]:
        result, _, _ = cluster(script, *args)
        assert result.returncode == 0, result.stderr
    result, state, calls = cluster("02-join-k3s-agent.sh", "--node", "3", env={"FAKE_FAILURE": failure})
    assert result.returncode != 0
    assert set(state["nodes"]) == {"1", "2"}
    assert not state["pods"]
    assert "3" not in {call["node"] for call in calls}


def test_matching_installation_is_reused_and_conflicting_config_is_preserved(cluster):
    result, _, _ = cluster("01-install-k3s-server.sh", "--node", "1")
    assert result.returncode == 0, result.stderr
    result, state, _ = cluster("01-install-k3s-server.sh", "--node", "1")
    assert result.returncode == 0, result.stderr
    assert len(state["installs"]) == 1
    config = cluster.root / "host-1/config/config.yaml"
    config.write_text("# Existing cluster managed separately\n")
    result, state, _ = cluster("01-install-k3s-server.sh", "--node", "1")
    assert result.returncode != 0
    assert "refusing to overwrite" in result.stderr
    assert config.read_text() == "# Existing cluster managed separately\n"
    assert len(state["installs"]) == 1


def test_storage_corruption_is_a_failure_and_probe_claims_are_retained(cluster):
    bootstrap(cluster)
    result, state, _ = cluster("verify-cluster.sh", "--check-storage", env={"FAKE_FAILURE": "storage"})
    assert result.returncode != 0
    assert "did not survive" in result.stderr
    assert not state["pods"]
    assert len(state["pvcs"]) == 1


def test_token_is_written_only_to_private_agent_file(cluster):
    result, _, _ = cluster("01-install-k3s-server.sh", "--node", "1")
    assert result.returncode == 0, result.stderr
    result, _, calls = cluster("02-join-k3s-agent.sh", "--node", "2", trace=True)
    assert result.returncode == 0, result.stderr
    token = cluster.root / "host-2/config/vietpulse-agent-token"
    assert token.read_text().strip() == "K10test::agent:do-not-disclose"
    assert token.stat().st_mode & 0o777 == 0o600
    assert "K10test::agent:do-not-disclose" not in result.stdout + result.stderr + json.dumps(calls)
    result, state, _ = cluster("02-join-k3s-agent.sh", "--node", "2", env={"K3S_TOKEN": "different-token"})
    assert result.returncode != 0
    assert "different-token" not in result.stdout + result.stderr
    assert token.read_text().strip() == "K10test::agent:do-not-disclose"
    assert len(state["installs"]) == 2


@pytest.mark.parametrize("invalid_state", ["extra-node", "missing-label", "missing-namespace", "storage-map"])
def test_verification_rejects_foundation_drift(cluster, invalid_state):
    bootstrap(cluster)
    state = json.loads(cluster.state_file.read_text())
    if invalid_state == "extra-node":
        state["nodes"]["5"] = "v1.35.9+k3s1"
    elif invalid_state == "missing-label":
        state["labels"]["3"].remove("vietpulse.io/streaming=true")
    elif invalid_state == "missing-namespace":
        state["namespaces"].remove("processing")
    else:
        state["storage_config"]["nodePathMap"][2]["paths"] = ["/unexpected/storage"]
    cluster.state_file.write_text(json.dumps(state))
    result, _, _ = cluster("verify-cluster.sh")
    assert result.returncode != 0


def test_existing_claims_prevent_changing_the_storage_map(cluster):
    result, state, _ = cluster("01-install-k3s-server.sh", "--node", "1")
    assert result.returncode == 0, result.stderr
    original = {
        "nodePathMap": [
            {
                "node": "DEFAULT_PATH_FOR_NON_LISTED_NODES",
                "paths": [str(cluster.root / "host-1/storage/vietpulse/local-path")],
            }
        ]
    }
    state["storage_config"] = original
    state["pvcs"]["important-application-data"] = {"contents": "keep this"}
    cluster.state_file.write_text(json.dumps(state))
    result, state, _ = cluster("01-install-k3s-server.sh", "--node", "1")
    assert result.returncode != 0
    assert "Existing local-path PVCs" in result.stderr
    assert state["storage_config"] == original
    assert state["pvcs"]["important-application-data"]["contents"] == "keep this"


def test_agent_refuses_missing_server_before_contacting_the_target(cluster):
    result, state, calls = cluster("02-join-k3s-agent.sh", "--node", "2")
    assert result.returncode != 0
    assert not state["nodes"]
    assert {call["node"] for call in calls} == {"1"}


def test_version_mismatch_stops_verification(cluster):
    result, state, _ = cluster("01-install-k3s-server.sh", "--node", "1")
    assert result.returncode == 0, result.stderr
    result, state, calls = cluster("02-join-k3s-agent.sh", "--node", "2", env={"FAKE_FAILURE": "version"})
    assert result.returncode != 0
    assert "version or InternalIP" in result.stderr
    assert set(state["nodes"]) == {"1"}
    assert {call["node"] for call in calls} == {"1"}


@pytest.mark.parametrize(("script", "args"), COMMANDS)
def test_dry_runs_do_not_connect_or_request_tokens(operator, script, args):
    result, calls = operator(script, *args, "--dry-run", env={"K3S_TOKEN": "never-print-this"})
    assert result.returncode == 0, result.stderr
    assert "DRY RUN" in result.stdout
    assert "never-print-this" not in result.stdout + result.stderr
    assert not calls


@pytest.mark.parametrize("script", ["01-install-k3s-server.sh", "02-join-k3s-agent.sh"])
@pytest.mark.parametrize("args", [[], ["--node", "all"], ["--node", "5"], ["--node", "2", "--node", "3"]])
def test_installations_require_one_explicit_valid_target(operator, script, args):
    result, calls = operator(script, *args, "--dry-run")
    assert result.returncode != 0
    assert "node" in result.stderr.lower()
    assert not calls


@pytest.mark.parametrize(("script", "args"), COMMANDS)
def test_blank_example_is_rejected_before_ssh(operator, script, args):
    result, calls = operator(script, *args, env_file=ROOT / "infra/.env.example")
    assert result.returncode != 0
    assert "NODE1_IP is required" in result.stderr
    assert not calls
