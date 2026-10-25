"""Exercise preflight with a local SSH substitute; never contact a real node."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "infra/scripts/00-preflight.sh"


@pytest.fixture
def operator(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "ssh.jsonl"
    fake_ssh = bin_dir / "ssh"
    fake_ssh.write_text(
        f"#!{sys.executable}\n"
        "import json, os, shlex, subprocess, sys\n"
        "body = sys.stdin.read()\n"
        "with open(os.environ['SSH_LOG'], 'a') as log:\n"
        "    log.write(json.dumps({'args': sys.argv[1:], 'body': body}) + '\\n')\n"
        "node = sys.argv[-2].split('.')[-1]\n"
        "if os.environ.get('FAIL_SSH') == node:\n"
        "    sys.exit(255)\n"
        "env = dict(os.environ, FAKE_NODE=node)\n"
        "result = subprocess.run(shlex.split(sys.argv[-1]), input=body, text=True, env=env)\n"
        "sys.exit(result.returncode)\n"
    )
    fake_ssh.chmod(0o755)
    # Execute the actual remote shell program, substituting only OS/network tools.
    fake_tool = bin_dir / "fake-tool"
    fake_tool.write_text(
        f"#!{sys.executable}\n"
        "import os, pathlib, sys\n"
        "name = pathlib.Path(sys.argv[0]).name\n"
        "node = os.environ['FAKE_NODE']\n"
        "args = sys.argv[1:]\n"
        "with open(os.environ['PROBE_LOG'], 'a') as log:\n"
        "    import json\n"
        "    log.write(json.dumps({'node': node, 'command': name, 'args': args}) + '\\n')\n"
        "if name == 'hostname':\n"
        "    print('vp-node-' + (os.environ.get('WRONG_HOST') or node))\n"
        "elif name == 'awk' and '/etc/os-release' in args:\n"
        "    version = os.environ.get('FAKE_OS_VERSION', '24.04')\n"
        "    print(version if any('VERSION_ID' in arg for arg in args) else 'ubuntu')\n"
        "elif name == 'lscpu':\n"
        "    print('Architecture: x86_64\\nCPU(s): 16\\nCore(s) per socket: 8')\n"
        "elif name == 'free':\n"
        "    print('              total used free shared buff/cache available')\n"
        "    print('Mem: 16384 1024 15360 0 0 15360')\n"
        "elif name == 'df':\n"
        "    print('Filesystem 1K-blocks Used Available Use% Mounted on')\n"
        "    print('/dev/test 1000000 100000 900000 10% /')\n"
        "elif name == 'findmnt':\n"
        "    print('/dev/test ext4 /')\n"
        "elif name == 'ip':\n"
        "    print('eth0 UP 192.168.50.' + node + '/24')\n"
        "elif name == 'ping':\n"
        "    sys.exit(1 if os.environ.get('FAIL_PING') == node else 0)\n"
        "elif name == 'sudo':\n"
        "    if args != ['-n', 'ufw', 'status', 'verbose']:\n"
        "        sys.exit('Unexpected privileged command: ' + repr(args))\n"
        "    if os.environ.get('DENY_SUDO'):\n"
        "        sys.exit(1)\n"
        "    print('Status: ' + os.environ.get('UFW_STATUS', 'inactive'))\n"
        "elif name == 'ufw':\n"
        "    sys.exit('ufw must only be inspected through sudo -n')\n"
        "else:\n"
        "    sys.exit('Unexpected probe: ' + name)\n"
    )
    fake_tool.chmod(0o755)
    for command in ("hostname", "awk", "lscpu", "free", "df", "findmnt", "ip", "ping", "sudo", "ufw"):
        (bin_dir / command).symlink_to(fake_tool)
    config = {"FIREWALL_POLICY": "ufw-disabled"}
    for node in range(1, 5):
        config.update(
            {
                f"NODE{node}_IP": f"192.168.50.{node}",
                f"NODE{node}_USER": "operator",
                f"NODE{node}_STORAGE_PATH": str(tmp_path),
            }
        )

    def run(*args, overrides=None, extra="", env=None):
        values = config | (overrides or {})
        env_file = tmp_path / "inventory.env"
        env_file.write_text("".join(f"{key}={value}\n" for key, value in values.items()) + extra)
        result = subprocess.run(
            ["bash", str(SCRIPT), "--env-file", str(env_file), *args],
            env=os.environ
            | {
                "PATH": f"{bin_dir}:{os.environ['PATH']}",
                "SSH_LOG": str(log),
                "PROBE_LOG": str(tmp_path / "probes.jsonl"),
            }
            | (env or {}),
            capture_output=True,
            text=True,
            timeout=20,
        )
        calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        return result, calls

    return run


def test_preflight_requires_explicit_read_only_mode(operator):
    result, calls = operator()
    assert result.returncode != 0
    assert "--read-only" in result.stderr
    assert not calls


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("NODE4_USER", ""),
        ("NODE1_USER", "-oProxyCommand=touch"),
        ("NODE2_IP", "192.0.2.10"),
        ("NODE2_IP", "127.0.0.1"),
        ("NODE2_IP", "192.168.50.999"),
        ("NODE2_IP", "192.168.50.1"),
        ("NODE3_STORAGE_PATH", "relative/path"),
        ("FIREWALL_POLICY", ""),
        ("FIREWALL_POLICY", "automatic"),
        ("SSH_PORT", "0"),
        ("SSH_PORT", "65536"),
        ("SSH_PORT", "22;touch /tmp/nope"),
    ],
)
def test_invalid_inventory_is_rejected_before_ssh(operator, key, value):
    result, calls = operator("--read-only", overrides={key: value})
    assert result.returncode != 0
    assert key in result.stderr
    assert not calls


def test_config_is_never_executed_or_echoed(operator, tmp_path):
    marker = tmp_path / "injected"
    payload = f"$(touch {marker})"
    result, calls = operator("--read-only", overrides={"NODE1_USER": payload})
    assert result.returncode != 0
    assert not marker.exists()
    assert payload not in result.stderr + result.stdout
    assert not calls


@pytest.mark.parametrize(
    "extra", ["NODE1_USER=second\n", "K3S_TOKEN=do-not-print-this\n", "touch /tmp/nope\n"]
)
def test_duplicate_unknown_or_executable_config_lines_are_rejected(operator, extra):
    result, calls = operator("--read-only", extra=extra)
    assert result.returncode != 0
    assert "do-not-print-this" not in result.stderr + result.stdout
    assert not calls


def test_read_only_inventory_inspects_all_four_nodes(operator, tmp_path):
    result, calls = operator("--read-only")
    assert result.returncode == 0, result.stderr + result.stdout
    assert len(calls) == 4
    for node, call in enumerate(calls, 1):
        assert f"operator@192.168.50.{node}" in call["args"]
        assert "StrictHostKeyChecking=yes" in call["args"]
        assert "BatchMode=yes" in call["args"]
        assert "UpdateHostKeys=no" in call["args"]
        assert f"vp-node-{node}" in result.stdout
    for field in ("CPU", "RAM", "Filesystem", "ufw-disabled", "UNVERIFIED"):
        assert field in result.stdout
    probes = [json.loads(line) for line in (tmp_path / "probes.jsonl").read_text().splitlines()]
    ping_pairs = {(probe["node"], probe["args"][-1]) for probe in probes if probe["command"] == "ping"}
    assert ping_pairs == {
        (str(source), f"192.168.50.{target}")
        for source in range(1, 5)
        for target in range(1, 5)
        if source != target
    }


def test_ssh_failure_stops_before_dependent_nodes(operator):
    result, calls = operator("--read-only", env={"FAIL_SSH": "2"})
    assert result.returncode != 0
    assert len(calls) == 2
    assert "Node 2" in result.stderr


@pytest.mark.parametrize("env", [{"FAIL_PING": "1"}, {"WRONG_HOST": "other"}, {"DENY_SUDO": "1"}])
def test_failed_host_prerequisites_are_not_reported_as_success(operator, env):
    result, calls = operator("--read-only", env=env)
    assert result.returncode != 0
    assert len(calls) == 1


def test_firewall_policy_mismatch_fails_without_changing_it(operator):
    result, _ = operator("--read-only", env={"UFW_STATUS": "active"})
    assert result.returncode != 0
    assert "firewall" in (result.stderr + result.stdout).lower()


def test_enabled_ufw_reports_rules_and_leaves_port_verification_pending(operator):
    result, calls = operator(
        "--read-only", overrides={"FIREWALL_POLICY": "ufw-rules"}, env={"UFW_STATUS": "active"}
    )
    assert result.returncode == 0, result.stderr + result.stdout
    assert len(calls) == 4
    assert "UNVERIFIED" in result.stdout


def test_unknown_cli_option_never_contacts_hosts(operator):
    result, calls = operator("--read-only", "--install")
    assert result.returncode != 0
    assert not calls


@pytest.mark.parametrize("version", ["22.04", "20.04"])
def test_os_support_is_checked_on_the_host(operator, version):
    result, calls = operator("--read-only", env={"FAKE_OS_VERSION": version})
    assert result.returncode == (0 if version == "22.04" else 1)
    assert len(calls) == (4 if version == "22.04" else 1)


def test_missing_storage_path_is_not_created(operator, tmp_path):
    path = tmp_path / "not-created"
    result, calls = operator("--read-only", overrides={"NODE1_STORAGE_PATH": str(path)})
    assert result.returncode != 0
    assert len(calls) == 1
    assert not path.exists()
    assert "must already exist" in result.stderr


def test_example_requires_actual_operator_inputs():
    result = subprocess.run(
        ["bash", str(SCRIPT), "--env-file", str(ROOT / "infra/.env.example"), "--read-only"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode != 0
    assert "NODE1_IP is required" in result.stderr


def test_local_credentials_are_ignored_but_example_and_secrets_readme_are_trackable():
    ignored = [
        "infra/.env",
        "infra/.env.local",
        "infra/local/inventory.env",
        "infra/reports/preflight.txt",
        "infra/secrets/token",
        "k8s/secrets/generated.yaml",
        "infra/kubeconfig.yaml",
        "infra/admin.kubeconfig",
        "infra/operator.key",
    ]
    trackable = ["infra/.env.example", "k8s/secrets/README.md", "infra/versions.yaml"]
    result = subprocess.run(
        ["git", "check-ignore", "--no-index", "--stdin"],
        input="\n".join(ignored + trackable) + "\n",
        capture_output=True,
        text=True,
        cwd=ROOT,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert set(result.stdout.splitlines()) == set(ignored)
