"""Guard against false-positive storage health checks and destructive initialization."""

import copy
import os
import runpy
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
STORAGE = ROOT / "k8s/base/storage"


@pytest.fixture
def checks():
    return runpy.run_path(str(ROOT / "infra/scripts/lib/storage_checks.py"))


@pytest.fixture
def resources():
    objects = []
    for name, node, size in (
        ("hdfs-namenode", 4, "10Gi"),
        ("hdfs-datanode-batch", 2, "200Gi"),
        ("hdfs-datanode-data", 4, "200Gi"),
        ("hive-postgresql", 4, "10Gi"),
    ):
        objects.extend(
            [
                {
                    "kind": "Pod",
                    "metadata": {"name": f"{name}-0", "labels": {"app": name}},
                    "spec": {
                        "nodeName": f"vp-node-{node}",
                        "volumes": [{"persistentVolumeClaim": {"claimName": f"data-{name}-0"}}],
                    },
                    "status": {"conditions": [{"type": "Ready", "status": "True"}]},
                },
                {
                    "kind": "PersistentVolumeClaim",
                    "metadata": {"name": f"data-{name}-0"},
                    "spec": {"storageClassName": "local-path", "resources": {"requests": {"storage": size}}},
                    "status": {"phase": "Bound"},
                },
            ]
        )
    objects.append(
        {
            "kind": "Pod",
            "metadata": {"name": "hive-metastore-example", "labels": {"app": "hive-metastore"}},
            "spec": {"nodeName": "vp-node-4"},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        }
    )
    return objects


def test_accepts_expected_ready_pods_and_bound_storage(checks, resources):
    checks["validate_resources"](resources)


@pytest.mark.parametrize("failure", ["placement", "unready", "pvc", "size", "extra", "terminating"])
def test_rejects_unhealthy_storage(checks, resources, failure):
    pod = resources[0]
    if failure == "placement":
        pod["spec"]["nodeName"] = "vp-node-1"
    elif failure == "unready":
        pod["status"]["conditions"][0]["status"] = "False"
    elif failure == "pvc":
        resources[1]["status"]["phase"] = "Pending"
    elif failure == "size":
        resources[1]["spec"]["resources"]["requests"]["storage"] = "1Gi"
    elif failure == "extra":
        resources.append(copy.deepcopy(pod))
    else:
        pod["metadata"]["deletionTimestamp"] = "2026-10-25T00:00:00Z"
    with pytest.raises(ValueError):
        checks["validate_resources"](resources)


def test_hdfs_report_requires_two_distinct_live_datanodes(checks):
    report = "Live datanodes (2):\nName: 10.42.1.2:9866\nName: 10.42.2.2:9866\nDead datanodes (0):"
    checks["validate_report"](report)
    for broken in (report.replace("(2)", "(1)"), report.replace("10.42.2.2", "10.42.1.2")):
        with pytest.raises(ValueError):
            checks["validate_report"](broken)


def test_namenode_initialization_refuses_existing_unrecognized_data(tmp_path):
    config = next(yaml.safe_load_all((STORAGE / "hdfs.yaml").read_text()))["data"]
    name = tmp_path / "name"
    name.mkdir()
    (name / "existing-data").write_text("keep me")
    result = subprocess.run(
        ["bash", "-c", config["start-hdfs.sh"], "sh", "namenode"],
        env=os.environ | {"HDFS_DATA_ROOT": str(tmp_path)},
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode != 0
    assert "refusing" in result.stderr.lower()
    assert (name / "existing-data").read_text() == "keep me"


def test_storage_dry_run_does_not_contact_hosts(tmp_path):
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
        ["bash", str(ROOT / "infra/scripts/verify-storage.sh"), "--env-file", str(inventory), "--dry-run"],
        capture_output=True,
        text=True,
        env=os.environ | {"PATH": f"{tmp_path}:{os.environ['PATH']}"},
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert "DRY RUN" in result.stdout


def test_local_cleanup_tolerates_an_already_exited_process():
    smoke = runpy.run_path(str(ROOT / "tests/integration/storage_local_smoke.py"))
    process = subprocess.Popen(["true"], start_new_session=True)
    process.wait(timeout=5)
    smoke["stop_process"](process)


def test_rejects_a_drifted_container_image(checks, resources):
    with pytest.raises(ValueError, match="image"):
        checks["validate_resources"](resources, {"hadoop": "apache/hadoop:3.3.6"})


def test_images_and_jdbc_artifact_match_the_version_pins():
    pins = yaml.safe_load((ROOT / "infra/versions.yaml").read_text())["storage"]
    documents = [
        obj
        for name in ("hdfs.yaml", "metastore.yaml")
        for obj in yaml.safe_load_all((STORAGE / name).read_text())
    ]
    for obj in documents:
        if obj["kind"] not in ("StatefulSet", "Deployment", "Job"):
            continue
        spec = obj["spec"]["template"]["spec"]
        assert "kubernetes.io/hostname" not in spec["nodeSelector"]
        assert spec["automountServiceAccountToken"] is False
        for container in spec.get("containers", []) + spec.get("initContainers", []):
            assert container["image"] in {
                pins[key] for key in ("hadoop_image", "metastore_image", "postgresql_image", "download_image")
            }
            if container["name"] == "postgresql-jdbc":
                command = container["command"][-1]
                assert f"postgresql-{pins['postgresql_jdbc']}.jar" in command
                assert pins["postgresql_jdbc_sha256"] in command
        for env in spec["containers"][0].get("env", []):
            if env["name"] in ("POSTGRES_USER", "POSTGRES_DB", "PGUSER", "PGDATABASE"):
                assert env["valueFrom"]["secretKeyRef"]["name"] == "hive-metastore-db"
    for service in (obj for obj in documents if obj["kind"] == "Service"):
        assert service["spec"].get("type", "ClusterIP") == "ClusterIP"


def test_metastore_credentials_are_xml_escaped_and_never_logged(tmp_path):
    config = next(yaml.safe_load_all((STORAGE / "metastore.yaml").read_text()))["data"]
    for filename in ("core-site.xml", "hdfs-site.xml"):
        (tmp_path / filename).write_text("<configuration/>")
    secret = 'a&b<c>d"e'
    (tmp_path / "password").write_text(secret)
    (tmp_path / "schema-state").write_text("existing")
    for name in ("schematool", "hive"):
        binary = tmp_path / name
        binary.write_text("#!/bin/sh\nexit 0\n")
        binary.chmod(0o755)
    result = subprocess.run(
        ["bash", "-c", config["start-metastore.sh"]],
        capture_output=True,
        text=True,
        timeout=5,
        env=os.environ
        | {
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "HADOOP_CONF_DIR": str(tmp_path),
            "HIVE_RUNTIME_CONF": str(tmp_path / "runtime"),
            "PGPASSWORD_FILE": str(tmp_path / "password"),
            "HIVE_SCHEMA_STATE": str(tmp_path / "schema-state"),
            "PGUSER": "hive",
            "PGDATABASE": "metastore",
        },
    )
    assert result.returncode == 0, result.stderr
    rendered = tmp_path / "runtime/hive-site.xml"
    properties = {prop.findtext("name"): prop.findtext("value") for prop in ET.parse(rendered).getroot()}
    assert properties["javax.jdo.option.ConnectionPassword"] == secret
    assert rendered.stat().st_mode & 0o777 == 0o600
    assert secret not in result.stdout + result.stderr
