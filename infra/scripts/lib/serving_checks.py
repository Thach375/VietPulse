"""Validate serving snapshots and authenticated API responses without printing credentials."""

import argparse
import json
from pathlib import Path


def validate_health(body):
    if (
        body.get("status") not in ("green", "yellow")
        or body.get("number_of_nodes") != 1
        or body.get("timed_out") is not False
        or body.get("unassigned_primary_shards", 0) != 0
    ):
        raise ValueError("Elasticsearch requires one healthy node and no unassigned primary shards")
    # The API reports active primaries separately from all active shards.
    if body.get("active_primary_shards", -1) < 0 or body.get("initializing_shards", 0) != 0:
        raise ValueError("Elasticsearch primary shard allocation has not settled")


def validate_document(body, marker):
    if body.get("found") is not True or body.get("_source", {}).get("marker") != marker:
        raise ValueError("Elasticsearch verification document is missing or changed")


def validate_search(body, marker):
    if body.get("timed_out", False) or body.get("_shards", {}).get("failed", 0):
        raise ValueError("Elasticsearch returned an incomplete search")
    hits = body.get("hits", {})
    if (
        hits.get("total", {}).get("value") != 1
        or len(hits.get("hits", [])) != 1
        or hits["hits"][0].get("_source", {}).get("marker") != marker
    ):
        raise ValueError("Elasticsearch search did not return the expected verification document")


def validate_kibana(body):
    status = body.get("status", {})
    if (
        status.get("overall", {}).get("level") != "available"
        or status.get("core", {}).get("elasticsearch", {}).get("level") != "available"
    ):
        raise ValueError("Kibana or its Elasticsearch connection is unavailable")


def validate_resources(objects, pins):
    for kind, name, component in (
        ("MongoDBCommunity", "vietpulse-mongodb", "mongodb"),
        ("Elasticsearch", "vietpulse-es", "elasticsearch"),
        ("Kibana", "vietpulse-kibana", "kibana"),
    ):
        resource = next(
            (obj for obj in objects if obj["kind"] == kind and obj["metadata"]["name"] == name), None
        )
        if not resource or resource["spec"].get("version") != pins[component]:
            raise ValueError(f"Missing or drifted {component} resource")
        spec = resource["spec"]
        if component == "elasticsearch":
            count = sum(item.get("count", 0) for item in spec.get("nodeSets", []))
        else:
            count = spec.get("members" if component == "mongodb" else "count")
        if count != 1:
            raise ValueError(f"{component} must have one instance")
    pods = {}
    for component, expected_sizes in (
        ("mongodb", ["1Gi", "40Gi"]),
        ("elasticsearch", ["80Gi"]),
        ("kibana", []),
    ):
        matching = [
            obj
            for obj in objects
            if obj["kind"] == "Pod"
            and obj["metadata"].get("labels", {}).get("vietpulse.io/component") == component
        ]
        if len(matching) != 1:
            raise ValueError(f"Expected exactly one {component} pod")
        pod = matching[0]
        ready = any(
            item.get("type") == "Ready" and item.get("status") == "True"
            for item in pod.get("status", {}).get("conditions", [])
        )
        if (
            not ready
            or pod["metadata"].get("deletionTimestamp")
            or pod["spec"].get("nodeName") != "vp-node-4"
        ):
            raise ValueError(f"{component} must be ready on Node 4")
        container_name = "mongod" if component == "mongodb" else component
        container = next((c for c in pod["spec"]["containers"] if c["name"] == container_name), {})
        expected = pins[f"{component}_image"]
        if container.get("image") != expected:
            raise ValueError(f"{component} image differs from its pin")
        if expected_sizes:
            claims = {
                v["persistentVolumeClaim"]["claimName"]
                for v in pod["spec"].get("volumes", [])
                if "persistentVolumeClaim" in v
            }
            pvcs = [
                obj
                for obj in objects
                if obj["kind"] == "PersistentVolumeClaim" and obj["metadata"]["name"] in claims
            ]
            if (
                not pvcs
                or len(pvcs) != len(claims)
                or any(
                    pvc.get("status", {}).get("phase") != "Bound"
                    or pvc["spec"].get("storageClassName") != "local-path"
                    for pvc in pvcs
                )
            ):
                raise ValueError(f"{component} requires Bound local-path storage")
            sizes = sorted(pvc["spec"]["resources"]["requests"]["storage"] for pvc in pvcs)
            if sizes != expected_sizes:
                raise ValueError(f"{component} claim sizes differ from {expected_sizes}")
        pods[component] = pod["metadata"]["name"]
    for obj in objects:
        if obj["kind"] == "Service" and obj["spec"].get("type", "ClusterIP") != "ClusterIP":
            raise ValueError("Serving database services must remain internal")
    return pods


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=["resources", "health", "document", "search", "kibana"])
    parser.add_argument("file", type=Path)
    parser.add_argument("--marker", default="")
    parser.add_argument("--pins", type=Path)
    args = parser.parse_args()
    body = json.loads(args.file.read_text())
    if args.kind == "resources":
        import yaml

        if not args.pins:
            parser.error("--pins is required for resource validation")
        pins = yaml.safe_load(args.pins.read_text())["serving"]
        print(json.dumps(validate_resources(body["items"], pins)))
    elif args.kind in ("document", "search"):
        {"document": validate_document, "search": validate_search}[args.kind](body, args.marker)
    else:
        {"health": validate_health, "kibana": validate_kibana}[args.kind](body)


if __name__ == "__main__":
    main()
