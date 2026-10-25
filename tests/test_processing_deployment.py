"""The pinned Spark operator and host-free smoke command have safe defaults."""

import copy
import json
import os
import runpy
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from common.envelope import make_envelope

ROOT = Path(__file__).resolve().parents[1]
RENDER = runpy.run_path(str(ROOT / "infra/scripts/render_processing.py"))["render"]
CHECKS = runpy.run_path(str(ROOT / "infra/scripts/lib/processing_checks.py"))


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


@pytest.fixture
def snapshot():
    objects = []
    for folder in ("processing", "ingestion"):
        for path in (ROOT / "k8s/base" / folder).glob("*.yaml"):
            if path.name in ("operator-values.yaml", "kustomization.yaml"):
                continue
            for document in yaml.safe_load_all(path.read_text()):
                for obj in document.get("items", [document]):
                    obj["metadata"]["namespace"] = folder
                    objects.append(obj)
    objects = RENDER(
        objects,
        {
            "spark": "ghcr.io/test/spark@sha256:" + "a" * 64,
            "crawler": "ghcr.io/test/crawler@sha256:" + "b" * 64,
        },
        "registry",
    )
    for application in [
        obj for obj in objects if obj["kind"] == "SparkApplication" and not obj["spec"]["suspend"]
    ]:
        application["status"] = {"applicationState": {"state": "RUNNING"}}
        for role in ("driver", "executor"):
            objects.append(
                {
                    "kind": "Pod",
                    "metadata": {
                        "name": application["metadata"]["name"] + role,
                        "namespace": "processing",
                        "labels": application["spec"][role]["labels"],
                    },
                    "spec": {"nodeName": "vp-node-3"},
                    "status": {"conditions": [{"type": "Ready", "status": "True"}]},
                }
            )
    for component in ("controller", "webhook"):
        labels = {"test-component": component}
        objects.append(
            {
                "kind": "Deployment",
                "metadata": {"name": "spark-operator-" + component, "namespace": "processing"},
                "spec": {
                    "selector": {"matchLabels": labels},
                    "template": {
                        "spec": {
                            "nodeSelector": {"vietpulse.io/control": "true"},
                            "containers": [{"image": "ghcr.io/kubeflow/spark-operator/controller:2.5.2"}],
                        }
                    },
                },
            }
        )
        objects.append(
            {
                "kind": "Pod",
                "metadata": {"name": component, "namespace": "processing", "labels": labels},
                "spec": {"nodeName": "vp-node-1"},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
        )
    return objects


def test_renderer_activates_only_streaming_and_runtime_accepts_healthy_snapshot(snapshot):
    CHECKS["validate_snapshot"](snapshot)
    for app in [obj for obj in snapshot if obj["kind"] == "SparkApplication"]:
        assert app["spec"]["imagePullSecrets"] == ["registry"]
    assert all(obj["spec"]["suspend"] for obj in snapshot if obj["kind"] == "CronJob")


@pytest.mark.parametrize("failure", ["artifact", "placement", "checkpoint", "resources", "heavy", "pod"])
def test_runtime_rejects_missing_or_unsafe_processing_state(snapshot, failure):
    app = next(obj for obj in snapshot if obj["kind"] == "SparkApplication")
    if failure == "artifact":
        app["spec"]["image"] = "REQUIRED_SPARK_IMAGE"
    elif failure == "placement":
        app["spec"]["driver"]["nodeSelector"] = {"vietpulse.io/control": "true"}
    elif failure == "checkpoint":
        app["spec"]["arguments"][app["spec"]["arguments"].index("--checkpoint-location") + 1] = (
            "/tmp/checkpoint"
        )
    elif failure == "resources":
        app["spec"]["executor"]["memory"] = "16g"
    elif failure == "heavy":
        next(
            obj
            for obj in snapshot
            if obj["kind"] == "SparkApplication" and obj["metadata"]["name"] == "batch-clean"
        )["spec"]["suspend"] = False
    else:
        next(obj for obj in snapshot if obj["kind"] == "Pod")["spec"]["nodeName"] = "vp-node-1"
    with pytest.raises(ValueError):
        CHECKS["validate_snapshot"](snapshot)


def test_renderer_requires_real_digest_inputs():
    with pytest.raises(ValueError, match="published"):
        RENDER([], {"spark": "", "crawler": ""})


def test_crawler_receipt_must_match_real_kafka_value():
    envelope = make_envelope("news_article", "42", datetime.now(UTC), {"title": "tin thật"})
    receipt = {
        "topic": "raw.news.articles",
        "key": "news_article:42",
        "partition": 0,
        "offset": 42,
        "envelope": envelope,
    }
    logs = json.dumps({"crawler_receipt": receipt})
    CHECKS["validate_receipt"](logs, json.dumps(envelope))
    changed = copy.deepcopy(envelope)
    changed["payload"]["title"] = "different"
    with pytest.raises(ValueError, match="differs"):
        CHECKS["validate_receipt"](logs, json.dumps(changed))
