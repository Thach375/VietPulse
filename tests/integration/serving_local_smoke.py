"""Run real authenticated MongoDB, Elasticsearch and Kibana on loopback.

Uses pinned official distributions, temporary data/configuration and small heaps.
Stops only its own process groups. Logs/data stay in a private temporary directory.
This does not exercise MCK/ECK reconciliation, Kubernetes scheduling or PVCs.
"""

import argparse
import base64
import json
import os
import runpy
import secrets
import shutil
import signal
import socket
import ssl
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.request import Request, urlopen

import yaml

ROOT = Path(__file__).resolve().parents[2]
CHECKS = runpy.run_path(str(ROOT / "infra/scripts/lib/serving_checks.py"))


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("mongod", "mongosh", "elasticsearch-home", "kibana-home"):
        parser.add_argument(f"--{name}", required=True, type=Path)
    args = parser.parse_args()
    pins = yaml.safe_load((ROOT / "infra/versions.yaml").read_text())["serving"]
    config = next(yaml.safe_load_all((ROOT / "k8s/base/serving/databases.yaml").read_text()))["data"]
    work = Path(tempfile.mkdtemp(prefix="vietpulse-serving-local-"))
    work.chmod(0o700)
    print(f"Local serving logs/data: {work}", flush=True)
    for name, text in config.items():
        (work / name).write_text(text)
    for name in ("mongo-data", "es-data", "es-logs", "kibana-data", "home"):
        (work / name).mkdir()
    env = os.environ | {
        "HOME": str(work / "home"),
        "ES_PATH_CONF": str(work / "es-conf"),
        "ES_JAVA_OPTS": "-Xms512m -Xmx512m -XX:ActiveProcessorCount=2",
    }
    processes, logs = {}, []

    def run(command, input_text=None, extra=None, timeout=90):
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
            raise RuntimeError(f"{Path(command[0]).name} failed; inspect {work}:\n{stderr[-2500:]}")
        return stdout

    for command, expected in (
        ([args.mongod, "--version"], pins["mongodb"]),
        ([args.mongosh, "--version"], pins["mongosh"]),
        ([args.elasticsearch_home / "bin/elasticsearch", "--version"], pins["elasticsearch"]),
        ([args.kibana_home / "bin/kibana", "--version"], pins["kibana"]),
    ):
        if expected not in run(command):
            raise ValueError(f"Distribution does not match version pin {expected}")
    listeners = [socket.socket() for _ in range(4)]
    try:
        for listener in listeners:
            listener.bind(("127.0.0.1", 0))
        mongoport, esport, transportport, kbport = [s.getsockname()[1] for s in listeners]
    finally:
        for listener in listeners:
            listener.close()
    password = secrets.token_urlsafe(32)
    mongo_password = secrets.token_urlsafe(32)
    for name, value in (
        ("es-password", password),
        ("mongo-password", mongo_password),
        ("mongo-key", base64.b64encode(secrets.token_bytes(512)).decode()),
    ):
        path = work / name
        path.write_text(value)
        path.chmod(0o600)
    marker = f"vp-serving-{secrets.token_hex(16)}"
    mongo_uri = f"mongodb://127.0.0.1:{mongoport}/?directConnection=true&serverSelectionTimeoutMS=5000"
    mongo_config = {
        "uri": mongo_uri,
        "passwordFile": str(work / "mongo-password"),
        "marker": marker,
        "write": True,
    }
    (work / "mongo-probe.json").write_text(json.dumps(mongo_config))
    env |= {
        "VP_MONGO_CONFIG": str(work / "mongo-probe.json"),
        "VP_ES_USERNAME": "vp_probe",
        "VP_ES_PASSWORD_FILE": str(work / "es-password"),
        "VP_ES_CA": str(work / "tls.crt"),
        "VP_KIBANA_CA": str(work / "tls.crt"),
        "VP_ES_URL": f"https://127.0.0.1:{esport}",
        "VP_KIBANA_URL": f"https://127.0.0.1:{kbport}",
    }
    run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "2",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
            "-keyout",
            work / "tls.key",
            "-out",
            work / "tls.crt",
        ]
    )
    shutil.copytree(args.elasticsearch_home / "config", work / "es-conf")
    es_config = {
        "cluster.name": "vietpulse-es",
        "node.name": "local-data",
        "network.host": "127.0.0.1",
        "http.port": esport,
        "transport.port": transportport,
        "discovery.type": "single-node",
        "path.data": str(work / "es-data"),
        "path.logs": str(work / "es-logs"),
        "node.store.allow_mmap": False,
        "xpack.security.enabled": True,
        "xpack.security.http.ssl.enabled": True,
        "xpack.security.http.ssl.key": str(work / "tls.key"),
        "xpack.security.http.ssl.certificate": str(work / "tls.crt"),
        "xpack.security.transport.ssl.enabled": True,
        "xpack.security.transport.ssl.key": str(work / "tls.key"),
        "xpack.security.transport.ssl.certificate": str(work / "tls.crt"),
        "xpack.security.transport.ssl.certificate_authorities": [str(work / "tls.crt")],
        "xpack.security.transport.ssl.verification_mode": "certificate",
    }
    # Elasticsearch restricts TLS files to its configuration directory.
    for name in ("tls.key", "tls.crt"):
        shutil.copy2(work / name, work / "es-conf" / name)
    for key in list(es_config):
        if key.endswith((".key", ".certificate")):
            es_config[key] = str(work / "es-conf" / Path(es_config[key]).name)
        elif key.endswith(".certificate_authorities"):
            es_config[key] = [str(work / "es-conf/tls.crt")]
    (work / "es-conf/elasticsearch.yml").write_text(yaml.safe_dump(es_config))
    run(
        [args.elasticsearch_home / "bin/elasticsearch-users", "useradd", "vp_probe", "-r", "superuser"],
        input_text=f"{password}\n{password}\n",
    )
    kb_password = secrets.token_urlsafe(32)
    kb_config = {
        "server.host": "127.0.0.1",
        "server.port": kbport,
        "server.name": "vietpulse-kibana",
        "server.ssl.enabled": True,
        "server.ssl.certificate": str(work / "tls.crt"),
        "server.ssl.key": str(work / "tls.key"),
        "path.data": str(work / "kibana-data"),
        "elasticsearch.hosts": [env["VP_ES_URL"]],
        "elasticsearch.username": "kibana_system",
        "elasticsearch.password": kb_password,
        "elasticsearch.ssl.certificateAuthorities": [str(work / "tls.crt")],
        "telemetry.enabled": False,
        "newsfeed.enabled": False,
        "xpack.security.encryptionKey": secrets.token_hex(32),
        "xpack.encryptedSavedObjects.encryptionKey": secrets.token_hex(32),
        "xpack.reporting.encryptionKey": secrets.token_hex(32),
    }
    (work / "kibana.yml").write_text(yaml.safe_dump(kb_config))
    (work / "kibana.yml").chmod(0o600)

    def start(name, command, extra=None):
        log = (work / f"{name}.log").open("a")
        logs.append(log)
        processes[name] = subprocess.Popen(
            list(map(str, command)),
            env=env | (extra or {}),
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def stop(name):
        stop_process(processes.pop(name))

    def wait_for(probe):
        deadline = time.monotonic() + 240
        last_error = None
        while time.monotonic() < deadline:
            try:
                return probe()
            except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
                last_error = error
                if any(p.poll() is not None for p in processes.values()):
                    raise RuntimeError(f"A serving process exited; inspect {work}") from error
                time.sleep(2)
        raise RuntimeError(f"Serving service did not become ready: {last_error}")

    def shell(js):
        return run([args.mongosh, "--quiet", mongo_uri, "--eval", js])

    def mongo_probe():
        return run([args.mongosh, "--quiet", "--nodb", work / "mongo-probe.js"])

    def http_probe(kind):
        body = json.loads(run(["sh", work / "elastic-probe.sh", kind, marker]))
        if kind in ("document", "search"):
            CHECKS[f"validate_{kind}"](body, marker)
        elif kind in ("health", "kibana"):
            CHECKS[f"validate_{kind}"](body)
        return body

    mongo_command = [
        args.mongod,
        "--dbpath",
        work / "mongo-data",
        "--bind_ip",
        "127.0.0.1",
        "--port",
        str(mongoport),
        "--replSet",
        "vietpulse-mongodb",
        "--auth",
        "--keyFile",
        work / "mongo-key",
        "--wiredTigerCacheSizeGB",
        "0.25",
    ]
    es_command = [args.elasticsearch_home / "bin/elasticsearch"]
    kb_command = [args.kibana_home / "bin/kibana", "--config", work / "kibana.yml"]
    try:
        start("mongodb", mongo_command)
        wait_for(lambda: shell("db.adminCommand({ping:1})"))
        shell(
            "rs.initiate({_id:'vietpulse-mongodb',members:[{_id:0,host:'localhost:" + str(mongoport) + "'}]})"
        )
        wait_for(lambda: shell("if (!db.hello().isWritablePrimary) throw new Error('Not primary yet')"))
        # The localhost exception initializes the first user; passwords never enter arguments.
        bootstrap = work / "mongo-bootstrap.js"
        bootstrap.write_text(
            "const fs=require('fs'); const admin=db.getSiblingDB('admin');"
            "const pw=fs.readFileSync(process.env.VP_MONGO_PASSWORD,'utf8');"
            "admin.createUser({user:'bootstrap_admin',pwd:pw,roles:[{role:'userAdminAnyDatabase',db:'admin'}]});"
            "admin.auth('bootstrap_admin',pw); admin.createUser({user:'vietpulse',"
            "pwd:fs.readFileSync(process.env.VP_MONGO_PASSWORD,'utf8'),"
            "roles:[{role:'readWrite',db:'vietpulse'}]});"
        )
        run(
            [args.mongosh, "--quiet", mongo_uri, bootstrap],
            extra={"VP_MONGO_PASSWORD": str(work / "mongo-password")},
        )
        mongo_probe()
        print(
            "PASS: authenticated MongoDB majority/journal write and read; unauthenticated read rejected.",
            flush=True,
        )
        start("elasticsearch", es_command)
        wait_for(lambda: http_probe("health"))
        context = ssl.create_default_context(cafile=str(work / "tls.crt"))
        credential = base64.b64encode(f"vp_probe:{password}".encode()).decode()
        request = Request(
            env["VP_ES_URL"] + "/_security/user/kibana_system/_password",
            data=json.dumps({"password": kb_password}).encode(),
            method="POST",
            headers={"Authorization": "Basic " + credential, "Content-Type": "application/json"},
        )
        with urlopen(request, context=context, timeout=30) as response:
            response.read()
        http_probe("write")
        http_probe("unauth")
        http_probe("document")
        http_probe("search")
        print("PASS: Elasticsearch HTTPS/authentication, indexing and search.", flush=True)
        start("kibana", kb_command, {"NODE_OPTIONS": "--max-old-space-size=768"})
        wait_for(lambda: http_probe("kibana"))
        print("PASS: Kibana HTTPS API reports an available Elasticsearch connection.", flush=True)
        mongo_config["write"] = False
        (work / "mongo-probe.json").write_text(json.dumps(mongo_config))
        stop("mongodb")
        start("mongodb", mongo_command)
        wait_for(mongo_probe)
        stop("elasticsearch")
        start("elasticsearch", es_command)
        wait_for(lambda: http_probe("health"))
        http_probe("document")
        http_probe("search")
        wait_for(lambda: http_probe("kibana"))
        stop("kibana")
        start("kibana", kb_command, {"NODE_OPTIONS": "--max-old-space-size=768"})
        wait_for(lambda: http_probe("kibana"))
        print(
            "PASS: individual restarts preserved MongoDB/Elasticsearch markers and Kibana connectivity.",
            flush=True,
        )
        print("Local serving smoke PASSED; operator/PVC/four-node checks remain pending.", flush=True)
    finally:
        for name in reversed(list(processes)):
            stop(name)
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()
