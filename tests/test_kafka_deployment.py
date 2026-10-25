"""Check deployment contracts and reject false positives in the live Kafka verifier."""

import copy
import os
import re
import runpy
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
MESSAGING = ROOT / "k8s/base/messaging"


@pytest.fixture
def checks():
    return runpy.run_path(str(ROOT / "infra/scripts/lib/kafka_checks.py"))


@pytest.fixture
def resources():
    objects = list(yaml.safe_load_all((MESSAGING / "kafka.yaml").read_text()))
    objects += list(yaml.safe_load_all((MESSAGING / "topics.yaml").read_text()))
    for obj in objects:
        obj["metadata"]["generation"] = 2
        obj["status"] = {"observedGeneration": 2, "conditions": [{"type": "Ready", "status": "True"}]}
        if obj["kind"] == "Kafka":
            obj["status"]["kafkaVersion"] = "4.2.1"
    for pool, node, size in (("batch", 2, "40Gi"), ("streaming", 3, "25Gi"), ("data", 4, "40Gi")):
        claim = f"data-0-vietpulse-{pool}-0"
        objects.extend(
            [
                {
                    "kind": "Pod",
                    "metadata": {
                        "name": f"vietpulse-{pool}-0",
                        "labels": {"strimzi.io/cluster": "vietpulse", "strimzi.io/pool-name": pool},
                    },
                    "spec": {
                        "nodeName": f"vp-node-{node}",
                        "volumes": [{"persistentVolumeClaim": {"claimName": claim}}],
                    },
                    "status": {"conditions": [{"type": "Ready", "status": "True"}]},
                },
                {
                    "kind": "PersistentVolumeClaim",
                    "metadata": {"name": claim},
                    "spec": {"storageClassName": "local-path", "resources": {"requests": {"storage": size}}},
                    "status": {"phase": "Bound", "capacity": {"storage": size}},
                },
            ]
        )
    labels = {1: "control", 2: "batch", 3: "streaming", 4: "data"}
    for node, role in labels.items():
        objects.append(
            {
                "kind": "Node",
                "metadata": {
                    "name": f"vp-node-{node}",
                    "labels": {f"vietpulse.io/{role}": "true", "vietpulse.io/kafka": str(node != 1).lower()},
                },
            }
        )
    objects.append(
        {
            "kind": "Deployment",
            "metadata": {"name": "strimzi-cluster-operator"},
            "spec": {
                "template": {
                    "spec": {
                        "nodeSelector": {"vietpulse.io/control": "true"},
                        "containers": [{"image": "quay.io/strimzi/operator:1.2.0"}],
                    }
                }
            },
            "status": {"readyReplicas": 1},
        }
    )
    return objects


def test_topics_preserve_the_existing_contract():
    topics = {
        obj["metadata"]["name"]: obj["spec"]
        for obj in yaml.safe_load_all((MESSAGING / "topics.yaml").read_text())
    }
    # Read the executable dev contract so a future topic/partition/retention change cannot drift silently.
    contract = (ROOT / "docker/kafka/create-topics.sh").read_text()
    expected = {}
    for name, partitions, days in re.findall(
        r"^create ([\w.]+)\s+(\d+)\s+\$\(\((\d+) \* DAY\)\)", contract, re.M
    ):
        expected[name] = (int(partitions), int(days) * 86400000)
    assert len(expected) == 6
    checks = runpy.run_path(str(ROOT / "infra/scripts/lib/kafka_checks.py"))
    assert checks["TOPICS"] == expected
    assert set(topics) == set(expected)
    for name, (partitions, retention) in expected.items():
        assert topics[name]["partitions"] == partitions
        assert topics[name]["config"]["retention.ms"] == retention
        assert topics[name]["replicas"] == 3
        assert topics[name]["config"]["min.insync.replicas"] == 2


def test_accepts_healthy_resources(checks, resources):
    pins = yaml.safe_load((ROOT / "infra/versions.yaml").read_text())["messaging"]
    assert (
        checks["validate_resources"](resources, pins["kafka"], pins["operator_version"])
        == "vietpulse-batch-0"
    )


def test_role_affinity_selects_exactly_the_intended_worker(resources):
    nodes = [obj for obj in resources if obj["kind"] == "Node"]
    for pool, target in (("batch", "vp-node-2"), ("streaming", "vp-node-3"), ("data", "vp-node-4")):
        spec = next(
            obj["spec"]
            for obj in resources
            if obj["kind"] == "KafkaNodePool" and obj["metadata"]["name"] == pool
        )
        terms = spec["template"]["pod"]["affinity"]["nodeAffinity"][
            "requiredDuringSchedulingIgnoredDuringExecution"
        ]["nodeSelectorTerms"]
        selected = []
        for node in nodes:
            labels = node["metadata"]["labels"]
            if any(
                all(
                    expr["operator"] == "In" and labels.get(expr["key"]) in expr["values"]
                    for expr in term["matchExpressions"]
                )
                for term in terms
            ):
                selected.append(node["metadata"]["name"])
        assert selected == [target]


@pytest.mark.parametrize(
    "failure", ["placement", "stale", "pvc", "extra_broker", "retention", "pin", "roles"]
)
def test_rejects_unhealthy_resources(checks, resources, failure):
    kafka = next(obj for obj in resources if obj["kind"] == "Kafka")
    pod = next(obj for obj in resources if obj["kind"] == "Pod")
    if failure == "placement":
        pod["spec"]["nodeName"] = "vp-node-1"
    elif failure == "stale":
        kafka["status"]["observedGeneration"] = 1
    elif failure == "pvc":
        next(obj for obj in resources if obj["kind"] == "PersistentVolumeClaim")["status"]["phase"] = (
            "Pending"
        )
    elif failure == "extra_broker":
        resources.append(copy.deepcopy(pod))
    elif failure == "retention":
        next(obj for obj in resources if obj["kind"] == "KafkaTopic")["spec"]["config"]["retention.ms"] = 1
    elif failure == "pin":
        kafka["status"]["kafkaVersion"] = "4.0.0"
    else:
        next(obj for obj in resources if obj["kind"] == "KafkaNodePool")["spec"]["roles"] = ["broker"]
    with pytest.raises(ValueError):
        checks["validate_resources"](resources, "4.2.1", "1.2.0")


@pytest.fixture
def description(checks):
    lines = []
    for topic, (partitions, retention) in checks["TOPICS"].items():
        lines.append(
            f"Topic: {topic} TopicId: abc PartitionCount: {partitions} ReplicationFactor: 3 "
            f"Configs: min.insync.replicas=2,retention.ms={retention}"
        )
        for partition in range(partitions):
            lines.append(f"Topic: {topic} Partition: {partition} Leader: 0 Replicas: 0,1,2 Isr: 0,1,2")
    return "\n".join(lines)


def test_accepts_actual_in_sync_replicas(checks, description):
    checks["validate_topics"](description)


def test_topic_identity_comparison_ignores_the_isolated_probe(checks, description):
    initial = checks["validate_topics"](description)
    assert set(initial) == set(checks["TOPICS"])
    after_probe_creation = description + (
        "\nTopic: vietpulse.infra.verify TopicId: probe-id PartitionCount: 1 ReplicationFactor: 3"
    )
    assert checks["validate_topics"](after_probe_creation) == initial


@pytest.mark.parametrize("failure", ["under_replicated", "missing_partition", "min_isr", "missing_topic"])
def test_rejects_unhealthy_actual_topic_metadata(checks, description, failure):
    if failure == "under_replicated":
        description = description.replace("Isr: 0,1,2", "Isr: 0,1", 1)
    elif failure == "missing_partition":
        description = "\n".join(line for line in description.splitlines() if "Partition: 2" not in line)
    elif failure == "min_isr":
        description = description.replace("min.insync.replicas=2", "min.insync.replicas=1", 1)
    else:
        description = "\n".join(line for line in description.splitlines() if "raw.backfill" not in line)
    with pytest.raises(ValueError):
        checks["validate_topics"](description)


def test_dry_run_never_contacts_hosts(tmp_path):
    inventory = tmp_path / "inventory.env"
    inventory.write_text(
        "FIREWALL_POLICY=ufw-disabled\n"
        + "".join(
            f"NODE{n}_IP=192.168.50.{n}\nNODE{n}_USER=operator\nNODE{n}_STORAGE_PATH=/data\n"
            for n in range(1, 5)
        )
    )
    ssh = tmp_path / "ssh"
    log = tmp_path / "ssh-called"
    ssh.write_text(f"#!/bin/sh\ntouch '{log}'\nexit 99\n")
    ssh.chmod(0o755)
    result = subprocess.run(
        ["bash", str(ROOT / "infra/scripts/verify-kafka.sh"), "--env-file", str(inventory), "--dry-run"],
        capture_output=True,
        text=True,
        env=os.environ | {"PATH": f"{tmp_path}:{os.environ['PATH']}"},
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert "DRY RUN" in result.stdout
    assert not log.exists()


def test_local_smoke_waits_for_all_three_registered_brokers(monkeypatch):
    smoke = runpy.run_path(str(ROOT / "tests/integration/kafka_local_smoke.py"))
    responses = iter(
        [
            "127.0.0.1:1001 (id: 0 rack: null) -> (...)\n127.0.0.1:1002 (id: 1 rack: null) -> (...)",
            "127.0.0.1:1001 (id: 0 rack: null) -> (...)\n127.0.0.1:1002 (id: 1 rack: null) -> (...)\n"
            "127.0.0.1:1003 (id: 2 rack: null) -> (...)",
        ]
    )
    monkeypatch.setattr(smoke["time"], "sleep", lambda _: None)
    smoke["wait_for_brokers"](lambda: next(responses))
    assert next(responses, None) is None
