import copy
from urllib.error import HTTPError

import pytest

from infra.scripts import processing_control as control


class API:
    def __init__(self, failed_stage=None, locked=False):
        self.failed_stage = failed_stage
        self.lock = {"spec": {"holderIdentity": "someone"}} if locked else None
        self.started = []
        self.apps = {}

    def request(self, method, resource, name="", body=None):
        if resource == "configmaps":
            raise HTTPError("api", 404, "No human approval", {}, None)
        if resource == "leases":
            if method == "POST":
                if self.lock:
                    raise HTTPError("api", 409, "Conflict", {}, None)
                self.lock = copy.deepcopy(body)
            elif method == "GET":
                return self.lock
            elif method == "DELETE":
                self.lock = None
            return {}
        if method == "POST":
            self.started.append(body)
            name = body["metadata"]["name"]
            stage = name.rsplit("-", 1)[0]
            state = "FAILED" if stage == self.failed_stage else "COMPLETED"
            self.apps[name] = {"status": {"applicationState": {"state": state}}}
            return body
        if name in self.apps:
            return self.apps[name]
        return {
            "spec": {
                "suspend": True,
                "image": "ghcr.io/test/spark@sha256:" + "a" * 64,
                "arguments": [name.removeprefix("batch-")],
            }
        }


def test_heavy_stages_run_in_order_with_one_cutoff_and_release_lock():
    api = API()
    control.run(api, through_date="2026-10-24", holder="test")
    assert [app["spec"]["arguments"][0] for app in api.started] == ["clean", "analytics", "publish"]
    assert all("2026-10-24" in app["spec"]["arguments"] for app in api.started)
    assert all(app["spec"]["suspend"] is False for app in api.started)
    assert api.lock is None


def test_failed_stage_never_starts_publish():
    api = API(failed_stage="batch-analytics")
    with pytest.raises(RuntimeError, match="FAILED"):
        control.run(api)
    assert len(api.started) == 2
    assert api.lock is None


def test_manual_run_cannot_overlap_another_heavy_job():
    api = API(locked=True)
    with pytest.raises(RuntimeError, match="already holds"):
        control.run(api)
    assert api.started == []
    assert api.lock["spec"]["holderIdentity"] == "someone"


def test_reference_apply_without_human_approval_starts_nothing():
    api = API()
    with pytest.raises(HTTPError):
        control.run(api, mode="reference-apply")
    assert api.started == []
    assert api.lock is None


def test_uncertain_cancellation_retains_lock(monkeypatch):
    api = API()

    def timeout(*args):
        api.apps[api.started[-1]["metadata"]["name"]]["status"]["applicationState"]["state"] = "RUNNING"
        raise TimeoutError("not complete")

    monkeypatch.setattr(control, "wait_application", timeout)
    monkeypatch.setattr(control, "stop_application", lambda *args: (_ for _ in ()).throw(OSError("API down")))
    with pytest.raises(OSError):
        control.run(api)
    assert api.lock is not None
