"""The pinned Spark operator and host-free smoke command have safe defaults."""

import os
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_operator_watches_processing_with_driver_rbac_on_node_one():
    values = yaml.safe_load((ROOT / "k8s/base/processing/operator-values.yaml").read_text())
    assert values["spark"]["jobNamespaces"] == ["processing"]
    assert values["spark"]["serviceAccount"] == {"name": "spark", "create": True}
    assert values["spark"]["rbac"]["create"] is True
    for component in ("controller", "webhook"):
        assert values[component]["nodeSelector"] == {"vietpulse.io/control": "true"}


def test_smoke_dry_run_does_not_contact_cluster(tmp_path):
    inventory = tmp_path / "inventory.env"
    inventory.write_text(
        "FIREWALL_POLICY=ufw-disabled\n"
        + "".join(
            f"NODE{n}_IP=192.168.50.{n}\nNODE{n}_USER=operator\nNODE{n}_STORAGE_PATH=/data\n"
            for n in range(1, 5)
        )
    )
    ssh = tmp_path / "ssh"
    ssh.write_text("#!/bin/sh\nexit 99\n")
    ssh.chmod(0o755)
    result = subprocess.run(
        [
            "bash",
            str(ROOT / "infra/scripts/verify-processing.sh"),
            "--env-file",
            str(inventory),
            "--dry-run",
            "--smoke",
            "--check-pipelines",
            "--check-ingestion",
        ],
        env=os.environ | {"PATH": f"{tmp_path}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert "SparkPi" in result.stdout
    assert "crawler artifacts" in result.stdout
