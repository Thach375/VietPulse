"""Run three real Kafka processes on loopback; needs Java 17+ and a Kafka distribution.

This checks messaging and disk persistence on one host, not Strimzi or K3s scheduling.
Processes are stopped on exit; the unique temporary directory is retained for inspection.
"""

import argparse
import os
import re
import runpy
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CHECKS = runpy.run_path(str(ROOT / "infra/scripts/lib/kafka_checks.py"))


def wait_for_brokers(probe):
    deadline = time.monotonic() + 120
    last_error = None
    while time.monotonic() < deadline:
        try:
            if set(re.findall(r"\bid:\s*(\d+)", probe())) == {"0", "1", "2"}:
                return
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            last_error = error
        time.sleep(2)
    raise RuntimeError(f"Three brokers did not register within 120 seconds: {last_error}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-home", required=True, type=Path)
    args = parser.parse_args()
    kafka_bin = args.kafka_home.resolve() / "bin"
    work = Path(tempfile.mkdtemp(prefix="vietpulse-kafka-local-"))
    print(f"Local smoke logs/data: {work}", flush=True)
    env = os.environ | {"KAFKA_HEAP_OPTS": "-Xms64m -Xmx256m"}

    def cli(tool, *arguments, input_text=None, timeout=45):
        result = subprocess.run(
            [str(kafka_bin / tool), *map(str, arguments)],
            input=input_text,
            capture_output=True,
            text=True,
            env=env,
            timeout=timeout,
        )
        if result.returncode:
            raise RuntimeError(f"{tool} failed ({result.returncode}):\n{result.stdout}\n{result.stderr}")
        return result.stdout

    pins = yaml.safe_load((ROOT / "infra/versions.yaml").read_text())
    version = cli("kafka-topics.sh", "--version").split()[0]
    if version != pins["messaging"]["kafka"]:
        raise ValueError(f"Expected Kafka {pins['messaging']['kafka']}, got {version}")
    # Reserve six available loopback ports during configuration; release just before startup.
    sockets = [socket.socket() for _ in range(6)]
    try:
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        ports = [listener.getsockname()[1] for listener in sockets]
    finally:
        for listener in sockets:
            listener.close()
    bootstrap = ",".join(f"127.0.0.1:{port}" for port in ports[:3])
    controllers = ",".join(f"127.0.0.1:{port}" for port in ports[3:])
    initial = ",".join(
        f"{node}@127.0.0.1:{ports[node + 3]}:{cli('kafka-storage.sh', 'random-uuid').strip()}"
        for node in range(3)
    )
    cluster_id = cli("kafka-storage.sh", "random-uuid").strip()
    documents = list(yaml.safe_load_all((ROOT / "k8s/base/messaging/kafka.yaml").read_text()))
    kafka_config = next(doc for doc in documents if doc["kind"] == "Kafka")["spec"]["kafka"]["config"]
    topics = list(yaml.safe_load_all((ROOT / "k8s/base/messaging/topics.yaml").read_text()))
    processes, logs = {}, []

    def start(node):
        log = (work / f"broker-{node}.log").open("a")
        logs.append(log)
        processes[node] = subprocess.Popen(
            [str(kafka_bin / "kafka-server-start.sh"), str(work / f"broker-{node}.properties")],
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env | {"KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m"},
        )

    def stop(node):
        process = processes.pop(node)
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)

    def create_topics():
        for topic in topics:
            spec = topic["spec"]
            config = [
                argument
                for key, value in spec["config"].items()
                for argument in ("--config", f"{key}={value}")
            ]
            cli(
                "kafka-topics.sh",
                "--bootstrap-server",
                bootstrap,
                "--create",
                "--if-not-exists",
                "--topic",
                topic["metadata"]["name"],
                "--partitions",
                spec["partitions"],
                "--replication-factor",
                spec["replicas"],
                *config,
            )

    def wait_for_topics():
        deadline = time.monotonic() + 120
        last_error = None
        while time.monotonic() < deadline:
            try:
                description = cli("kafka-topics.sh", "--bootstrap-server", bootstrap, "--describe")
                return CHECKS["validate_topics"](description)
            except (ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
                last_error = error
                if any(process.poll() is not None for process in processes.values()):
                    raise RuntimeError(f"A Kafka process exited; inspect {work}") from error
                time.sleep(2)
        raise RuntimeError(f"Kafka did not converge: {last_error}")

    def consume(offset, marker):
        actual = cli(
            "kafka-console-consumer.sh",
            "--bootstrap-server",
            bootstrap,
            "--topic",
            "vietpulse.infra.verify",
            "--partition",
            "0",
            "--offset",
            offset,
            "--max-messages",
            "1",
            "--timeout-ms",
            "30000",
        ).strip()
        if actual != marker:
            raise ValueError(f"Message lost or changed at offset {offset}: {actual!r}")

    def produce(marker):
        cli(
            "kafka-console-producer.sh",
            "--bootstrap-server",
            bootstrap,
            "--topic",
            "vietpulse.infra.verify",
            "--property",
            "parse.key=true",
            "--property",
            "key.separator=:",
            "--producer-property",
            "acks=all",
            "--producer-property",
            "enable.idempotence=true",
            "--producer-property",
            "delivery.timeout.ms=30000",
            "--producer-property",
            "request.timeout.ms=10000",
            input_text=f"sentinel:{marker}\n",
        )

    try:
        # Kafka's documented initial-controller formatting creates a three-voter KRaft quorum.
        # https://kafka.apache.org/42/operations/kraft/#initial-controllers
        for node in range(3):
            config = {
                "process.roles": "broker,controller",
                "node.id": node,
                "controller.quorum.bootstrap.servers": controllers,
                "controller.listener.names": "CONTROLLER",
                "listeners": f"PLAINTEXT://127.0.0.1:{ports[node]},CONTROLLER://127.0.0.1:{ports[node + 3]}",
                "advertised.listeners": f"PLAINTEXT://127.0.0.1:{ports[node]}",
                "inter.broker.listener.name": "PLAINTEXT",
                "listener.security.protocol.map": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
                "log.dirs": work / f"data-{node}",
                "num.network.threads": 2,
                "num.io.threads": 2,
                "num.replica.fetchers": 1,
                "group.initial.rebalance.delay.ms": 0,
            } | kafka_config
            path = work / f"broker-{node}.properties"
            path.write_text(
                "".join(
                    f"{key}={str(value).lower() if isinstance(value, bool) else value}\n"
                    for key, value in config.items()
                )
            )
            cli(
                "kafka-storage.sh",
                "format",
                "--cluster-id",
                cluster_id,
                "--initial-controllers",
                initial,
                "--config",
                path,
            )
            start(node)
        wait_for_brokers(lambda: cli("kafka-broker-api-versions.sh", "--bootstrap-server", bootstrap))
        create_topics()
        topic_ids = wait_for_topics()
        print(
            "PASS: three brokers; six project topics match the contract with three in-sync replicas.",
            flush=True,
        )
        cli(
            "kafka-topics.sh",
            "--bootstrap-server",
            bootstrap,
            "--create",
            "--topic",
            "vietpulse.infra.verify",
            "--partitions",
            "1",
            "--replication-factor",
            "3",
            "--config",
            "min.insync.replicas=2",
            "--config",
            "cleanup.policy=compact",
        )
        first, second = f"vp-verify-{uuid.uuid4()}", f"vp-verify-{uuid.uuid4()}"
        produce(first)
        consume(0, first)
        stop(0)
        produce(second)
        consume(1, second)
        print("PASS: acks=all production and consumption continue with one broker stopped.", flush=True)
        start(0)
        wait_for_topics()
        create_topics()
        if wait_for_topics() != topic_ids:
            raise ValueError("Topic IDs changed after repeated topic creation")
        consume(0, first)
        consume(1, second)
        print(
            "PASS: broker restart and repeated topic creation preserved topic IDs and both unique messages.",
            flush=True,
        )
        print(
            f"Kafka {version} local smoke PASSED; Kubernetes placement/PVC checks remain pending.", flush=True
        )
    finally:
        for node in list(processes):
            stop(node)
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()
