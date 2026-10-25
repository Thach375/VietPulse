"""Validate live Kubernetes storage inventory and Hadoop's actual DataNode report."""

import argparse
import json
import re
from pathlib import Path

EXPECTED = {
    "hdfs-namenode": (4, "10Gi"),
    "hdfs-datanode-batch": (2, "200Gi"),
    "hdfs-datanode-data": (4, "200Gi"),
    "hive-postgresql": (4, "10Gi"),
    "hive-metastore": (4, None),
}


def validate_resources(objects, images=None):
    for app, (node, size) in EXPECTED.items():
        pods = [
            obj
            for obj in objects
            if obj["kind"] == "Pod" and obj["metadata"].get("labels", {}).get("app") == app
        ]
        if len(pods) != 1:
            raise ValueError(f"Expected exactly one {app} pod")
        pod = pods[0]
        ready = any(
            item.get("type") == "Ready" and item.get("status") == "True"
            for item in pod.get("status", {}).get("conditions", [])
        )
        if not ready or pod["metadata"].get("deletionTimestamp"):
            raise ValueError(f"{app} is not ready or is terminating")
        if pod["spec"].get("nodeName") != f"vp-node-{node}":
            raise ValueError(f"{app} must run on Node {node}")
        if images:
            component = {"hive-postgresql": "postgresql", "hive-metastore": "metastore"}.get(app, "hadoop")
            expected_image = images[component]
            containers = pod["spec"].get("containers", [])
            if len(containers) != 1 or containers[0].get("image") != expected_image:
                raise ValueError(f"{app} image does not match the version pin")
        if size:
            claims = [
                volume["persistentVolumeClaim"]["claimName"]
                for volume in pod["spec"].get("volumes", [])
                if "persistentVolumeClaim" in volume
            ]
            if len(claims) != 1:
                raise ValueError(f"{app} requires one persistent volume")
            pvc = next(
                (
                    obj
                    for obj in objects
                    if obj["kind"] == "PersistentVolumeClaim" and obj["metadata"]["name"] == claims[0]
                ),
                None,
            )
            if (
                not pvc
                or pvc.get("status", {}).get("phase") != "Bound"
                or pvc["spec"].get("storageClassName") != "local-path"
                or pvc["spec"].get("resources", {}).get("requests", {}).get("storage") != size
            ):
                raise ValueError(f"{app} requires a Bound {size} local-path claim")


def validate_report(report):
    live = re.search(r"Live datanodes \((\d+)\):", report)
    section = report.split("Live datanodes", 1)[-1].split("Dead datanodes", 1)[0]
    names = re.findall(r"^Name:\s+(\S+)", section, re.M)
    if not live or live[1] != "2" or len(names) != 2 or len(set(names)) != 2:
        raise ValueError("HDFS must report exactly two distinct live DataNodes")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=["resources", "report"])
    parser.add_argument("file", type=Path)
    for component in ("hadoop", "metastore", "postgresql"):
        parser.add_argument(f"--{component}-image")
    args = parser.parse_args()
    content = args.file.read_text()
    if args.kind == "resources":
        images = {
            component: getattr(args, f"{component}_image")
            for component in ("hadoop", "metastore", "postgresql")
        }
        if not all(images.values()):
            parser.error("Resource validation requires all three pinned images")
        validate_resources(json.loads(content)["items"], images)
    else:
        validate_report(content)


if __name__ == "__main__":
    main()
