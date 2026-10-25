"""Run real HDFS (two DataNodes), PostgreSQL and Hive Metastore on loopback.

Needs Java 8, Hadoop/Hive distributions, PostgreSQL binaries and the pinned JDBC jar.
No Docker, root, K3s, SSH or cluster inventory is needed. All data/logs stay in a
unique temporary directory. Only the processes started by this script are stopped.
"""

import argparse
import hashlib
import os
import re
import runpy
import secrets
import signal
import socket
import subprocess
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CHECKS = runpy.run_path(str(ROOT / "infra/scripts/lib/storage_checks.py"))


def stop_process(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        process.wait(timeout=10)
        return
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=10)


def write_config(path, template, overrides):
    tree = ET.fromstring(template)
    for prop in tree.findall("property"):
        key = prop.findtext("name")
        if key in overrides:
            prop.find("value").text = str(overrides.pop(key))
    for key, value in overrides.items():
        prop = ET.SubElement(tree, "property")
        ET.SubElement(prop, "name").text = key
        ET.SubElement(prop, "value").text = str(value)
    path.write_text(ET.tostring(tree, encoding="unicode"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("java-home", "hadoop-home", "hive-home", "postgres-bin", "jdbc-jar"):
        parser.add_argument(f"--{name}", required=True, type=Path)
    args = parser.parse_args()
    pins = yaml.safe_load((ROOT / "infra/versions.yaml").read_text())["storage"]
    if hashlib.sha256(args.jdbc_jar.read_bytes()).hexdigest() != pins["postgresql_jdbc_sha256"]:
        raise ValueError("JDBC driver does not match the pinned SHA-256")
    work = Path(tempfile.mkdtemp(prefix="vietpulse-storage-local-"))
    work.chmod(0o700)
    print(f"Local storage logs/data: {work}", flush=True)
    hdfs_config = next(yaml.safe_load_all((ROOT / "k8s/base/storage/hdfs.yaml").read_text()))["data"]
    hive_config = next(yaml.safe_load_all((ROOT / "k8s/base/storage/metastore.yaml").read_text()))["data"]
    for name, script in (hdfs_config | hive_config).items():
        if name.endswith(".sh"):
            (work / name).write_text(script)
    password = work / "password"
    password.write_text(secrets.token_urlsafe(24) + "&<>")
    password.chmod(0o600)
    (work / "schema-state").write_text("empty")
    env = os.environ | {
        "JAVA_HOME": str(args.java_home.resolve()),
        "HADOOP_HOME": str(args.hadoop_home.resolve()),
        "HIVE_HOME": str(args.hive_home.resolve()),
        "PATH": f"{args.hadoop_home.resolve()}/bin:{args.hive_home.resolve()}/bin:{os.environ['PATH']}",
        "HADOOP_USER_NAME": "hdfs",
        "HADOOP_HEAPSIZE_MAX": "256",
        "HADOOP_CLIENT_OPTS": "-Xmx256m -XX:ActiveProcessorCount=2",
        "HDFS_NAMENODE_OPTS": "-XX:ActiveProcessorCount=2",
        "HDFS_DATANODE_OPTS": "-XX:ActiveProcessorCount=2",
        "HIVE_AUX_JARS_PATH": str(args.jdbc_jar.resolve()),
        "HIVE_RUNTIME_CONF": str(work / "hive-conf"),
        "HIVE_SCHEMA_STATE": str(work / "schema-state"),
        "PGPASSWORD_FILE": str(password),
        "PGUSER": "hive",
        "PGDATABASE": "metastore",
        "PGHOST": "127.0.0.1",
        "METASTORE_BIND_HOST": "127.0.0.1",
    }
    processes, logs = {}, []

    def run(command, *, extra=None, input_text=None, timeout=120):
        process = subprocess.Popen(
            list(map(str, command)),
            env=env | (extra or {}),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            stdout, stderr = process.communicate(input=input_text, timeout=timeout)
        except subprocess.TimeoutExpired:
            stop_process(process)
            raise
        if process.returncode:
            # Credentials stay in files; do not print environment/configuration.
            raise RuntimeError(f"{command[0]} failed; inspect {work}:\n{stderr[-4000:]}")
        return stdout

    for binary, expected in (
        ([args.hadoop_home / "bin/hadoop", "version"], f"Hadoop {pins['hadoop']}"),
        ([args.hive_home / "bin/hive", "--version"], f"Hive {pins['hive_metastore']}"),
        ([args.postgres_bin / "postgres", "--version"], f"PostgreSQL) {pins['postgresql']}"),
    ):
        if expected not in run(binary):
            raise ValueError(f"Expected {expected}; check the supplied distribution")

    # Reserve ports together, then release just before starting the processes.
    listeners = [socket.socket() for _ in range(8)]
    try:
        for listener in listeners:
            listener.bind(("127.0.0.1", 0))
        ports = [listener.getsockname()[1] for listener in listeners]
    finally:
        for listener in listeners:
            listener.close()
    rpc, http, pgport, thrift, *dnports = ports
    env |= {
        "PGPORT": str(pgport),
        "METASTORE_PORT": str(thrift),
        "HIVE_METASTORE_URI": f"thrift://127.0.0.1:{thrift}",
    }
    for index in range(3):
        conf = work / f"conf-{index}"
        conf.mkdir()
        data = work / f"data-{index}"
        data.mkdir()
        write_config(
            conf / "core-site.xml", hdfs_config["core-site.xml"], {"fs.defaultFS": f"hdfs://127.0.0.1:{rpc}"}
        )
        overrides = {
            "dfs.namenode.name.dir": f"file://{data}/name",
            "dfs.datanode.data.dir": f"file://{data}/blocks",
            "dfs.namenode.rpc-address": f"127.0.0.1:{rpc}",
            "dfs.namenode.rpc-bind-host": "127.0.0.1",
            "dfs.namenode.http-address": f"127.0.0.1:{http}",
            "dfs.namenode.handler.count": "3",
            "dfs.datanode.ipc.address": "127.0.0.1:0",
        }
        if index:
            overrides |= {
                "dfs.datanode.address": f"127.0.0.1:{dnports[(index - 1) * 2]}",
                "dfs.datanode.http.address": f"127.0.0.1:{dnports[(index - 1) * 2 + 1]}",
            }
        write_config(conf / "hdfs-site.xml", hdfs_config["hdfs-site.xml"], overrides)
    env["HADOOP_CONF_DIR"] = str(work / "conf-0")

    def start(name, command, extra=None):
        log = (work / f"{name}.log").open("a")
        logs.append(log)
        pid_dir = work / f"pids-{name}"
        pid_dir.mkdir(exist_ok=True)
        processes[name] = subprocess.Popen(
            list(map(str, command)),
            env=env | {"HADOOP_PID_DIR": str(pid_dir)} | (extra or {}),
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def stop(name):
        process = processes.pop(name)
        stop_process(process)

    def wait_for(probe):
        deadline = time.monotonic() + 180
        last_error = None
        while time.monotonic() < deadline:
            try:
                probe()
                return
            except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
                last_error = error
                if any(process.poll() is not None for process in processes.values()):
                    raise RuntimeError(f"A storage process exited; inspect {work}") from error
                time.sleep(2)
        raise RuntimeError(f"Storage did not become ready: {last_error}")

    def hdfs(*arguments, **kwargs):
        return run([args.hadoop_home / "bin/hdfs", *arguments], **kwargs)

    def hdfs_ready():
        CHECKS["validate_report"](hdfs("dfsadmin", "-report", timeout=20))

    def catalog(read_only=False):
        command = ["bash", work / "probe-catalog.sh"]
        if read_only:
            command.append("DESCRIBE vp_infra_probe.storage_marker;")
        return run(
            command,
            extra={
                "HIVE_CONF_DIR": str(work / "hive-conf"),
                "HADOOP_USER_NAME": "hive",
            },
        )

    marker = f"vp-local-{secrets.token_hex(16)}"
    try:
        start(
            "namenode", ["bash", work / "start-hdfs.sh", "namenode"], {"HDFS_DATA_ROOT": str(work / "data-0")}
        )
        for index in (1, 2):
            start(
                f"datanode-{index}",
                ["bash", work / "start-hdfs.sh", "datanode"],
                {"HADOOP_CONF_DIR": str(work / f"conf-{index}")},
            )
        wait_for(hdfs_ready)
        run(["bash", work / "bootstrap-hdfs.sh"], timeout=180)
        run(["bash", work / "probe-hdfs.sh"], timeout=180)
        hdfs("dfs", "-put", "-", "/vietpulse/.infra-checks/local-marker", input_text=marker)
        print(
            "PASS: two live DataNodes, dataset/checkpoint directories and replicated HDFS round trip.",
            flush=True,
        )
        pgdata = work / "postgres"
        run(
            [
                args.postgres_bin / "initdb",
                "-D",
                pgdata,
                "-U",
                "hive",
                "--pwfile",
                password,
                "--auth-host=scram-sha-256",
                "--auth-local=trust",
                "--no-locale",
                "-E",
                "UTF8",
            ]
        )
        pgcommand = [
            args.postgres_bin / "postgres",
            "-D",
            pgdata,
            "-h",
            "127.0.0.1",
            "-p",
            str(pgport),
            "-k",
            str(work),
            "-c",
            "shared_buffers=32MB",
            "-c",
            "max_connections=20",
        ]
        start("postgres", pgcommand)
        wait_for(lambda: run([args.postgres_bin / "pg_isready", "-h", "127.0.0.1", "-p", str(pgport)]))
        run([args.postgres_bin / "createdb", "metastore"], extra={"PGPASSWORD": password.read_text()})
        start("metastore", ["bash", work / "start-metastore.sh"], {"HADOOP_USER_NAME": "hive"})
        wait_for(catalog)
        print(
            "PASS: PostgreSQL schema initialized; catalog DDL succeeded through Metastore Thrift.", flush=True
        )
        stop("metastore")
        # Keep the original "empty" flag to exercise a main-container-only restart.
        start("metastore", ["bash", work / "start-metastore.sh"], {"HADOOP_USER_NAME": "hive"})
        wait_for(lambda: catalog(read_only=True))
        stop("metastore")
        stop("postgres")
        start("postgres", pgcommand)
        wait_for(lambda: run([args.postgres_bin / "pg_isready", "-h", "127.0.0.1", "-p", str(pgport)]))
        (work / "schema-state").write_text("existing")
        start("metastore", ["bash", work / "start-metastore.sh"], {"HADOOP_USER_NAME": "hive"})
        wait_for(lambda: catalog(read_only=True))
        stop("datanode-1")
        start(
            "datanode-1",
            ["bash", work / "start-hdfs.sh", "datanode"],
            {"HADOOP_CONF_DIR": str(work / "conf-1")},
        )
        wait_for(hdfs_ready)
        stop("namenode")
        start(
            "namenode", ["bash", work / "start-hdfs.sh", "namenode"], {"HDFS_DATA_ROOT": str(work / "data-0")}
        )
        wait_for(hdfs_ready)
        hdfs("dfsadmin", "-safemode", "wait", timeout=90)
        if hdfs("dfs", "-cat", "/vietpulse/.infra-checks/local-marker") != marker:
            raise ValueError("HDFS marker changed after restart")
        # DESCRIBE alone proves existing metadata survived, without recreating it.
        output = catalog(read_only=True)
        if not re.search(r"^vp_marker\s+string", output, re.M):
            raise ValueError("Catalog metadata did not survive restart")
        print(
            "PASS: NameNode/DataNode and PostgreSQL/Metastore restarts preserved file and catalog metadata.",
            flush=True,
        )
        print(
            "Local storage smoke PASSED; four-node scheduling/PVC/network checks remain pending.", flush=True
        )
    finally:
        for name in reversed(list(processes)):
            stop(name)
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()
