"""Reject missing artifacts, invalid scheduling, shared checkpoints and fake ingestion proof."""

import argparse
import json
import re
from pathlib import Path

from common.envelope import kafka_key, validate_envelope

DIGEST = re.compile(r"[a-z0-9][a-z0-9./:_-]*@sha256:[a-f0-9]{64}")
TOPICS = {
    "news_article": "raw.news.articles",
    "news_comment": "raw.news.comments",
    "voz_post": "raw.forum.posts",
}
MEMORY = {
    "raw-ingest": ("768m", "256m", "1024m", "512m"),
    "speed-sentiment": ("1024m", "384m", "1536m", "768m"),
}


def validate_snapshot(objects, spark_version="4.0.4", operator_version="2.5.2"):
    def get(kind, name, namespace="processing"):
        found = [
            obj
            for obj in objects
            if obj["kind"] == kind
            and obj["metadata"]["name"] == name
            and obj["metadata"].get("namespace") == namespace
        ]
        if len(found) != 1:
            raise ValueError(f"Expected one {namespace}/{kind}/{name}")
        return found[0]

    images = get("ConfigMap", "vietpulse-image-pins")["data"]
    if any(not DIGEST.fullmatch(images.get(component, "")) for component in ("spark", "crawler")):
        raise ValueError("Published application artifacts are missing: require actual image digests")
    for component in ("controller", "webhook"):
        deployment = get("Deployment", f"spark-operator-{component}")
        template = deployment["spec"]["template"]
        image = template["spec"]["containers"][0]["image"]
        if (
            template["spec"].get("nodeSelector") != {"vietpulse.io/control": "true"}
            or image != f"ghcr.io/kubeflow/spark-operator/controller:{operator_version}"
        ):
            raise ValueError("Spark operator image or Node 1 placement drifted")
        selector = deployment["spec"]["selector"]["matchLabels"]
        pods = [
            obj
            for obj in objects
            if obj["kind"] == "Pod"
            and obj["metadata"].get("namespace") == "processing"
            and all(obj["metadata"].get("labels", {}).get(key) == value for key, value in selector.items())
            and not obj["metadata"].get("deletionTimestamp")
        ]
        if not pods or any(pod["spec"].get("nodeName") != "vp-node-1" or not ready(pod) for pod in pods):
            raise ValueError("Spark controller/webhook is not ready on Node 1")
    checkpoints = []
    for name in (
        "raw-ingest",
        "speed-sentiment",
        "batch-clean",
        "batch-analytics",
        "batch-publish",
        "reference-refresh",
    ):
        application = get("SparkApplication", name)
        spec = application["spec"]
        streaming = name in MEMORY
        expected_role = "streaming" if streaming else "batch"
        if spec.get("image") != images["spark"] or spec.get("sparkVersion") != spark_version:
            raise ValueError(f"Missing or drifted Spark artifact/version for {name}")
        if spec["driver"].get("nodeSelector") != {f"vietpulse.io/{expected_role}": "true"}:
            raise ValueError(f"Invalid driver placement for {name}")
        executor = spec["executor"]
        terms = (
            executor.get("affinity", {})
            .get("nodeAffinity", {})
            .get("requiredDuringSchedulingIgnoredDuringExecution", {})
            .get("nodeSelectorTerms", [])
        )
        roles = {
            expr["key"]
            for term in terms
            for expr in term.get("matchExpressions", [])
            if expr.get("operator") == "In" and expr.get("values") == ["true"]
        }
        if roles != {f"vietpulse.io/{expected_role}", "vietpulse.io/data"} or executor.get("instances") != 1:
            raise ValueError(f"Invalid executor placement/count for {name}")
        memory = (
            spec["driver"].get("memory"),
            spec["driver"].get("memoryOverhead"),
            executor.get("memory"),
            executor.get("memoryOverhead"),
        )
        if memory != MEMORY.get(name, ("1024m", "384m", "4096m", "1024m")):
            raise ValueError(f"Resource budget drifted for {name}")
        if streaming:
            if (
                spec.get("suspend")
                or application.get("status", {}).get("applicationState", {}).get("state") != "RUNNING"
            ):
                raise ValueError(f"{name} is not running; inspect image pull/submission/driver errors")
            args = spec["arguments"]
            flags = (
                ("--checkpoint-location",)
                if name == "raw-ingest"
                else (
                    "--checkpoint-location",
                    "--dedup-checkpoint",
                    "--window-checkpoint",
                    "--dedup-output-path",
                )
            )
            for flag in flags:
                if flag not in args:
                    raise ValueError(f"Explicit durable path {flag} missing for {name}")
                path = args[args.index(flag) + 1]
                if not path.startswith(
                    "hdfs://hdfs-namenode.storage.svc.cluster.local:8020/vietpulse/checkpoints/"
                ):
                    raise ValueError(f"{name} state must live in HDFS")
                checkpoints.append(path.rstrip("/"))
            for role in ("driver", "executor"):
                pods = [
                    obj
                    for obj in objects
                    if obj["kind"] == "Pod"
                    and obj["metadata"].get("namespace") == "processing"
                    and obj["metadata"].get("labels", {}).get("vietpulse.io/application") == name
                    and obj["metadata"].get("labels", {}).get("vietpulse.io/spark-role") == role
                    and not obj["metadata"].get("deletionTimestamp")
                ]
                nodes = {"vp-node-3"} if role == "driver" else {"vp-node-3", "vp-node-4"}
                if len(pods) != 1 or pods[0]["spec"].get("nodeName") not in nodes or not ready(pods[0]):
                    raise ValueError(f"{name} {role} is not ready in its allowed nodes")
        elif not spec.get("suspend") or spec.get("restartPolicy", {}).get("type") != "Never":
            raise ValueError("Heavy application templates must stay suspended with no automatic retry")
    if len(checkpoints) != len(set(checkpoints)):
        raise ValueError("Streaming queries share a checkpoint or intermediate directory")
    for name, namespace, role, image in (
        ("vietpulse-batch", "processing", "control", images["spark"]),
        ("reference-refresh-prepare", "processing", "control", images["spark"]),
        *(
            (name, "ingestion", "ingestion", images["crawler"])
            for name in ("news-articles", "news-comments", "voz")
        ),
    ):
        spec = get("CronJob", name, namespace)["spec"]
        pod = spec["jobTemplate"]["spec"]["template"]["spec"]
        if spec.get("concurrencyPolicy") != "Forbid" or pod.get("nodeSelector") != {
            f"vietpulse.io/{role}": "true"
        }:
            raise ValueError(f"Invalid schedule concurrency or placement for {name}")
        if pod["containers"][0]["image"] != image:
            raise ValueError(f"Published image missing for {name}")
    return images


def ready(pod):
    return any(
        item.get("type") == "Ready" and item.get("status") == "True"
        for item in pod.get("status", {}).get("conditions", [])
    )


def validate_receipt(logs, record=None):
    receipts = []
    for line in logs.splitlines():
        try:
            body = json.loads(line)
        except ValueError:
            continue
        if "crawler_receipt" in body:
            receipts.append(body["crawler_receipt"])
    if len(receipts) != 1:
        raise ValueError("A real crawler must produce exactly one acknowledged receipt")
    receipt = receipts[0]
    envelope = validate_envelope(receipt["envelope"])
    if receipt["topic"] != TOPICS[envelope["source"]] or receipt["key"] != kafka_key(envelope):
        raise ValueError("Crawler receipt does not match the contracted Kafka key/topic")
    if not isinstance(receipt["offset"], int) or receipt["offset"] < 0 or receipt["partition"] < 0:
        raise ValueError("Crawler acknowledgement lacks valid Kafka coordinates")
    if record is not None and json.loads(record) != envelope:
        raise ValueError("Kafka value at the acknowledged offset differs from the crawler envelope")
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("snapshot", "receipt"))
    parser.add_argument("file", type=Path)
    parser.add_argument("--record", type=Path)
    parser.add_argument("--spark-version", default="4.0.4")
    parser.add_argument("--operator-version", default="2.5.2")
    args = parser.parse_args()
    if args.kind == "snapshot":
        validate_snapshot(
            json.loads(args.file.read_text())["items"], args.spark_version, args.operator_version
        )
    else:
        receipt = validate_receipt(args.file.read_text(), args.record.read_text() if args.record else None)
        print(receipt["topic"], receipt["partition"], receipt["offset"])


if __name__ == "__main__":
    main()
