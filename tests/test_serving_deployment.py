"""Serving health checks must reject unavailable services and lost verification data."""

import copy
import os
import runpy
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SERVING = ROOT / "k8s/base/serving"


@pytest.fixture
def checks():
    return runpy.run_path(str(ROOT / "infra/scripts/lib/serving_checks.py"))


def test_single_node_health_accepts_only_allocated_primaries(checks):
    good = {
        "status": "yellow",
        "number_of_nodes": 1,
        "unassigned_primary_shards": 0,
        "active_primary_shards": 1,
        "timed_out": False,
    }
    checks["validate_health"](good)
    for change in (
        {"status": "red"},
        {"number_of_nodes": 2},
        {"timed_out": True},
        {"unassigned_primary_shards": 1},
        {"status": "unknown"},
    ):
        with pytest.raises(ValueError):
            checks["validate_health"](good | change)


def test_checks_stored_document_and_actual_search_hit(checks):
    checks["validate_document"]({"found": True, "_source": {"marker": "expected"}}, "expected")
    good = {"hits": {"total": {"value": 1}, "hits": [{"_source": {"marker": "expected"}}]}}
    checks["validate_search"](good, "expected")
    for broken in ({"found": False}, {"found": True, "_source": {"marker": "changed"}}):
        with pytest.raises(ValueError):
            checks["validate_document"](broken, "expected")
    bad = copy.deepcopy(good)
    bad["hits"]["hits"][0]["_source"]["marker"] = "changed"
    with pytest.raises(ValueError):
        checks["validate_search"](bad, "expected")
    for failure in ({"timed_out": True}, {"_shards": {"failed": 1}}):
        with pytest.raises(ValueError):
            checks["validate_search"](good | failure, "expected")


def test_kibana_must_report_a_working_elasticsearch_connection(checks):
    good = {"status": {"overall": {"level": "available"}, "core": {"elasticsearch": {"level": "available"}}}}
    checks["validate_kibana"](good)
    good["status"]["core"]["elasticsearch"]["level"] = "unavailable"
    with pytest.raises(ValueError):
        checks["validate_kibana"](good)


def test_dry_run_does_not_contact_hosts(tmp_path):
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
        ["bash", str(ROOT / "infra/scripts/verify-serving.sh"), "--env-file", str(inventory), "--dry-run"],
        env=os.environ | {"PATH": f"{tmp_path}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert "DRY RUN" in result.stdout


@pytest.fixture
def resources():
    pins = yaml.safe_load((ROOT / "infra/versions.yaml").read_text())["serving"]
    objects = [
        obj
        for obj in yaml.safe_load_all((SERVING / "databases.yaml").read_text())
        if obj["kind"] != "ConfigMap"
    ]
    for component, sizes in (("mongodb", ["40Gi", "1Gi"]), ("elasticsearch", ["80Gi"]), ("kibana", [])):
        volume = [
            {"persistentVolumeClaim": {"claimName": component + "-" + str(index)}}
            for index in range(len(sizes))
        ]
        objects.append(
            {
                "kind": "Pod",
                "metadata": {"name": component + "-0", "labels": {"vietpulse.io/component": component}},
                "spec": {
                    "nodeName": "vp-node-4",
                    "volumes": volume,
                    "containers": [
                        {
                            "name": "mongod" if component == "mongodb" else component,
                            "image": pins[component + "_image"],
                        }
                    ],
                },
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
        )
        for index, size in enumerate(sizes):
            objects.append(
                {
                    "kind": "PersistentVolumeClaim",
                    "metadata": {"name": component + "-" + str(index)},
                    "spec": {"storageClassName": "local-path", "resources": {"requests": {"storage": size}}},
                    "status": {"phase": "Bound"},
                }
            )
    objects.append({"kind": "Service", "metadata": {"name": "mongodb"}, "spec": {"clusterIP": "None"}})
    return objects, pins


def test_accepts_pinned_single_instance_resources(checks, resources):
    objects, pins = resources
    assert checks["validate_resources"](objects, pins)["mongodb"] == "mongodb-0"


@pytest.mark.parametrize(
    "failure",
    [
        "placement",
        "unready",
        "image",
        "pending",
        "size",
        "extra",
        "exposure",
        "scale",
        "version",
        "missing_cr",
        "missing_log_claim",
    ],
)
def test_rejects_unhealthy_serving_resources(checks, resources, failure):
    objects, pins = resources
    pod = next(obj for obj in objects if obj["kind"] == "Pod")
    claim = next(obj for obj in objects if obj["kind"] == "PersistentVolumeClaim")
    if failure == "placement":
        pod["spec"]["nodeName"] = "vp-node-1"
    elif failure == "unready":
        pod["status"]["conditions"][0]["status"] = "False"
    elif failure == "image":
        pod["spec"]["containers"][0]["image"] = "mongo:latest"
    elif failure == "pending":
        claim["status"]["phase"] = "Pending"
    elif failure == "size":
        claim["spec"]["resources"]["requests"]["storage"] = "1Gi"
    elif failure == "extra":
        objects.append(copy.deepcopy(pod))
    elif failure == "exposure":
        objects[-1]["spec"]["type"] = "NodePort"
    elif failure == "scale":
        objects[0]["spec"]["members"] = 3
    elif failure == "version":
        objects[0]["spec"]["version"] = "7.0.0"
    elif failure == "missing_log_claim":
        objects.remove(next(obj for obj in objects if obj.get("metadata", {}).get("name") == "mongodb-1"))
    else:
        objects.remove(objects[0])
    with pytest.raises(ValueError):
        checks["validate_resources"](objects, pins)


def test_manifests_keep_credentials_external_and_select_data_role():
    objects = list(yaml.safe_load_all((SERVING / "databases.yaml").read_text()))
    mongo, es, kibana = objects[1:]
    assert mongo["spec"]["users"][0]["passwordSecretRef"]["name"] == "vietpulse-mongodb-password"
    assert mongo["spec"]["security"]["authentication"]["modes"] == ["SCRAM"]
    assert mongo["spec"]["statefulSet"]["spec"]["template"]["spec"]["nodeSelector"] == {
        "vietpulse.io/data": "true"
    }
    assert es["spec"]["nodeSets"][0]["podTemplate"]["spec"]["nodeSelector"] == {"vietpulse.io/data": "true"}
    assert kibana["spec"]["podTemplate"]["spec"]["nodeSelector"] == {"vietpulse.io/data": "true"}
    assert kibana["spec"]["elasticsearchRef"]["name"] == es["metadata"]["name"]
    assert kibana["spec"]["secureSettings"] == [{"secretName": "vietpulse-kibana-keys"}]
    assert not any(obj["kind"] == "Secret" for obj in objects)
    for name in ("mongodb-values.yaml", "eck-values.yaml"):
        values = yaml.safe_load((SERVING / "operators" / name).read_text())
        spec = values.get("operator", values)
        assert spec["nodeSelector"] == {"vietpulse.io/control": "true"}
