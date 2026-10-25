"""Validate observed Kafka state. Uses only the Python standard library at runtime."""

import argparse
import json
import re
from pathlib import Path

# Keep aligned with docker/kafka/create-topics.sh; checked in test_kafka_deployment.py.
TOPICS = {
    "raw.news.articles": (3, 604800000),
    "raw.news.comments": (6, 604800000),
    "raw.forum.posts": (3, 604800000),
    "raw.backfill": (6, 172800000),
    "dlq.raw": (1, 1209600000),
    "alerts.sentiment": (1, 604800000),
}
POOLS = {"batch": (2, "40Gi"), "streaming": (3, "25Gi"), "data": (4, "40Gi")}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def ready(resource):
    return any(
        c["type"] == "Ready" and c["status"] == "True"
        for c in resource.get("status", {}).get("conditions", [])
    )


def reconciled(resource):
    require(ready(resource), f"{resource['kind']}/{resource['metadata']['name']} is not Ready")
    generation = resource["metadata"].get("generation")
    require(
        isinstance(generation, int)
        and generation > 0
        and resource["status"].get("observedGeneration") == generation,
        f"{resource['metadata']['name']} has stale reconciliation status",
    )


def validate_resources(objects, kafka_version, operator_version):
    def get(kind, name):
        matches = [obj for obj in objects if obj["kind"] == kind and obj["metadata"]["name"] == name]
        require(len(matches) == 1, f"Expected exactly one {kind}/{name}")
        return matches[0]

    kafka = get("Kafka", "vietpulse")
    reconciled(kafka)
    require(kafka["spec"]["kafka"]["version"] == kafka_version, "Kafka spec differs from version pin")
    require(kafka["status"].get("kafkaVersion") == kafka_version, "Running Kafka differs from version pin")
    operator = get("Deployment", "strimzi-cluster-operator")
    operator_pod = operator["spec"]["template"]["spec"]
    require(operator["status"].get("readyReplicas", 0) == 1, "Strimzi operator is not ready")
    require(operator_pod["nodeSelector"].get("vietpulse.io/control") == "true", "Operator must select Node 1")
    require(
        operator_pod["containers"][0]["image"] == f"quay.io/strimzi/operator:{operator_version}",
        "Strimzi image differs from version pin",
    )
    brokers = [
        obj
        for obj in objects
        if obj["kind"] == "Pod"
        and obj["metadata"].get("labels", {}).get("strimzi.io/cluster") == "vietpulse"
        and "strimzi.io/pool-name" in obj["metadata"].get("labels", {})
    ]
    require(len(brokers) == 3, "Expected exactly three Kafka broker/controller pods")
    for pool, (node, size) in POOLS.items():
        node_name = f"vp-node-{node}"
        labels = get("Node", node_name)["metadata"]["labels"]
        require(
            labels.get("vietpulse.io/kafka") == labels.get(f"vietpulse.io/{pool}") == "true",
            f"Wrong role labels on {node_name}",
        )
        spec = get("KafkaNodePool", pool)["spec"]
        require(
            spec["replicas"] == 1 and set(spec["roles"]) == {"broker", "controller"},
            f"Wrong roles/replicas for {pool}",
        )
        volume = spec["storage"]["volumes"][0]
        require(
            volume["size"] == size and volume.get("deleteClaim") is False,
            f"Wrong PVC size/retention for {pool}",
        )
        members = [pod for pod in brokers if pod["metadata"]["labels"]["strimzi.io/pool-name"] == pool]
        require(len(members) == 1, f"Expected one pod in pool {pool}")
        pod = members[0]
        require(
            ready(pod) and pod["spec"]["nodeName"] == node_name, f"Pool {pool} must be Ready on {node_name}"
        )
        claims = [
            v["persistentVolumeClaim"]["claimName"]
            for v in pod["spec"]["volumes"]
            if "persistentVolumeClaim" in v
        ]
        require(len(claims) == 1, f"Pool {pool} must mount one persistent data volume")
        pvc = get("PersistentVolumeClaim", claims[0])
        require(pvc["status"]["phase"] == "Bound", f"PVC {claims[0]} is not Bound")
        require(
            pvc["spec"]["storageClassName"] == "local-path"
            and pvc["spec"]["resources"]["requests"]["storage"] == size
            and pvc["status"]["capacity"]["storage"] == size,
            f"Wrong local-path capacity for {claims[0]}",
        )
    for name, (partitions, retention) in TOPICS.items():
        topic = get("KafkaTopic", name)
        reconciled(topic)
        spec = topic["spec"]
        require(
            spec["partitions"] == partitions and spec["replicas"] == 3,
            f"Wrong partitions/replication for {name}",
        )
        require(
            int(spec["config"]["min.insync.replicas"]) == 2
            and int(spec["config"]["retention.ms"]) == retention,
            f"Wrong minimum ISR/retention for {name}",
        )
    return next(
        pod["metadata"]["name"]
        for pod in brokers
        if pod["metadata"]["labels"]["strimzi.io/pool-name"] == "batch"
    )


def validate_topics(description):
    """Check Kafka's metadata, including every partition's actual replica and ISR sets."""
    headers, partitions = {}, {name: set() for name in TOPICS}
    for line in description.splitlines():
        topic_match = re.search(r"\bTopic:\s*(\S+)", line)
        if not topic_match or topic_match[1] not in TOPICS:
            continue
        name = topic_match[1]
        count, retention = TOPICS[name]
        header = re.search(r"PartitionCount:\s*(\d+)\s+ReplicationFactor:\s*(\d+)", line)
        if header:
            require(
                tuple(map(int, header.groups())) == (count, 3),
                f"Wrong live partition/replica count for {name}",
            )
            require(re.search(r"\bmin\.insync\.replicas=2(?:,|\s|$)", line), f"Wrong live min ISR for {name}")
            require(
                re.search(rf"\bretention\.ms={retention}(?:,|\s|$)", line), f"Wrong live retention for {name}"
            )
            headers[name] = True
        partition = re.search(
            r"Partition:\s*(\d+)\s+Leader:\s*(-?\d+)\s+Replicas:\s*([\d,]+)\s+Isr:\s*([\d,]+)", line
        )
        if partition:
            number, leader, replicas, isr = partition.groups()
            replica_set, isr_set = set(replicas.split(",")), set(isr.split(","))
            require(
                len(replica_set) == 3 and replica_set == isr_set and leader in isr_set,
                f"Unhealthy replicas/ISR for {name}:{number}",
            )
            partitions[name].add(int(number))
    require(set(headers) == set(TOPICS), "Missing live project topics")
    for name, (count, _) in TOPICS.items():
        require(partitions[name] == set(range(count)), f"Missing/unhealthy live partitions for {name}")
    identities = {
        name: topic_id
        for name, topic_id in re.findall(r"Topic:\s*(\S+)\s+TopicId:\s*(\S+)", description)
        if name in TOPICS
    }
    require(set(identities) == set(TOPICS), "Missing live project topic IDs")
    return identities


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["resources", "topics"])
    parser.add_argument("file", type=Path)
    parser.add_argument("--kafka-version")
    parser.add_argument("--operator-version")
    args = parser.parse_args()
    try:
        if args.mode == "resources":
            objects = json.loads(args.file.read_text())["items"]
            print(validate_resources(objects, args.kafka_version, args.operator_version))
        else:
            validate_topics(args.file.read_text())
    except (ValueError, KeyError, TypeError, IndexError) as error:
        parser.exit(1, f"ERROR: Kafka verification failed: {error}\n")


if __name__ == "__main__":
    main()
