"""Run heavy Spark stages under one namespace-wide lock, including manual runs.

A lock is never stolen on a timer: a dead controller leaves it for inspection.
Forbid on a CronJob alone does not exclude jobs started manually or by another
CronJob. All supported batch/reference entrypoints use this controller.
https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#concurrency-policy
"""

import argparse
import copy
import json
import os
import re
import signal
import ssl
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

TERMINAL = {"COMPLETED", "FAILED", "SUBMISSION_FAILED", "FAILED_SUBMISSION"}
LOCK = "vietpulse-heavy-batch"


class KubeAPI:
    def __init__(self, namespace="processing"):
        self.namespace = namespace
        self.endpoint = "https://kubernetes.default.svc"
        directory = Path("/var/run/secrets/kubernetes.io/serviceaccount")
        self.token = (directory / "token").read_text().strip()
        self.context = ssl.create_default_context(cafile=directory / "ca.crt")

    def request(self, method, resource, name="", body=None):
        groups = {"sparkapplications": "sparkoperator.k8s.io/v1beta2", "leases": "coordination.k8s.io/v1"}
        prefix = f"/apis/{groups[resource]}" if resource in groups else "/api/v1"
        path = f"{prefix}/namespaces/{self.namespace}/{resource}" + (f"/{name}" if name else "")
        request = Request(
            self.endpoint + path,
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
        )
        with urlopen(request, context=self.context, timeout=30) as response:
            return json.load(response)


def wait_application(api, name, timeout=7200):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        application = api.request("GET", "sparkapplications", name)
        state = application.get("status", {}).get("applicationState", {}).get("state")
        if state in TERMINAL:
            if state != "COMPLETED":
                raise RuntimeError(f"SparkApplication {name} failed: {state}")
            return application
        time.sleep(5)
    raise TimeoutError(f"SparkApplication {name} did not finish within {timeout}s")


def stop_application(api, name):
    api.request("DELETE", "sparkapplications", name, {"propagationPolicy": "Foreground"})
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            api.request("GET", "sparkapplications", name)
        except HTTPError as error:
            if error.code == 404:
                return
            raise
        time.sleep(3)
    raise TimeoutError("Cancellation did not finish; heavy batch lock retained for inspection")


def run(api, mode="batch", through_date=None, holder=None):
    run_id = uuid.uuid4().hex[:16]
    holder = holder or os.environ.get("POD_UID", run_id)
    through_date = through_date or (datetime.now(UTC).date() - timedelta(days=1)).isoformat()
    stages = (
        ("batch-clean", "batch-analytics", "batch-publish") if mode == "batch" else ("reference-refresh",)
    )
    approval = None
    if mode == "reference-apply":
        approval = api.request("GET", "configmaps", "reference-refresh-approval")["data"]
        if (
            not re.fullmatch(r"[a-z0-9-]{1,50}", approval.get("run_id", ""))
            or not re.fullmatch(r"[a-f0-9]{64}", approval.get("sha256", ""))
            or not approval.get("approved_by", "").strip()
        ):
            raise ValueError("Reference approval must identify the reviewed run, sha256 and approver")
    templates = {stage: api.request("GET", "sparkapplications", stage) for stage in stages}
    for stage, template in templates.items():
        if template["spec"].get("suspend") is not True:
            raise ValueError(f"Heavy template {stage} must be suspended")
        if not re.fullmatch(r"[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}", template["spec"].get("image", "")):
            raise ValueError(f"Published image digest missing for {stage}")
    lease = {
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {"name": LOCK},
        "spec": {
            "holderIdentity": holder,
            "acquireTime": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        },
    }
    try:
        api.request("POST", "leases", body=lease)
    except HTTPError as error:
        if error.code == 409:
            raise RuntimeError(
                "A heavy batch/reference run already holds the lock; no stage was started"
            ) from None
        raise
    active = None
    try:
        for stage in stages:
            application = {
                "apiVersion": "sparkoperator.k8s.io/v1beta2",
                "kind": "SparkApplication",
                "metadata": {
                    "name": f"{stage}-{run_id}",
                    "labels": {"vietpulse.io/heavy": "true", "vietpulse.io/run": run_id},
                },
                "spec": copy.deepcopy(templates[stage]["spec"]),
            }
            application["spec"]["suspend"] = False
            application["spec"]["restartPolicy"] = {"type": "Never"}
            application["spec"]["timeToLiveSeconds"] = 86400
            arguments = application["spec"].setdefault("arguments", [])
            candidate_id = approval["run_id"] if approval else run_id
            arguments.extend(["--run-id", candidate_id, "--through-date", through_date])
            if mode.startswith("reference-"):
                arguments.extend(["--reference-action", "apply" if approval else "prepare"])
            if approval:
                arguments.extend(
                    ["--approved-digest", approval["sha256"], "--approved-by", approval["approved_by"]]
                )
            active = application["metadata"]["name"]
            api.request("POST", "sparkapplications", body=application)
            wait_application(api, active)
            print(json.dumps({"stage": stage, "application": active, "state": "COMPLETED"}), flush=True)
            active = None
    finally:
        # Release only after the last child is terminal or fully cancelled.
        # If GET/cancellation itself fails, the lock remains and prevents overlap.
        if active:
            try:
                child = api.request("GET", "sparkapplications", active)
            except HTTPError as error:
                if error.code != 404:
                    raise
            else:
                if child.get("status", {}).get("applicationState", {}).get("state") not in TERMINAL:
                    stop_application(api, active)
        current = api.request("GET", "leases", LOCK)
        if current["spec"].get("holderIdentity") != holder:
            raise RuntimeError("Heavy lock ownership changed; refusing to release another run's lock")
        api.request("DELETE", "leases", LOCK)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("batch", "reference-prepare", "reference-apply"))
    parser.add_argument("--through-date")
    args = parser.parse_args()

    def interrupted(signum, frame):
        raise InterruptedError("Controller interrupted; cancelling active Spark stage")

    signal.signal(signal.SIGTERM, interrupted)
    run(KubeAPI(os.environ.get("POD_NAMESPACE", "processing")), args.mode, args.through_date)


if __name__ == "__main__":
    main()
