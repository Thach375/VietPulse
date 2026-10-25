import json

from jobs.setup_kibana import DASHBOARD_ID, saved_objects


def _by_id():
    return {obj["id"]: obj for obj in saved_objects()}


def test_object_ids_are_unique():
    objects = saved_objects()
    assert len({(o["type"], o["id"]) for o in objects}) == len(objects)


def test_dashboard_panels_reference_existing_objects():
    objects = _by_id()
    dashboard = objects[DASHBOARD_ID]
    panels = json.loads(dashboard["attributes"]["panelsJSON"])
    references = {ref["name"]: ref for ref in dashboard["references"]}
    assert len(panels) == len(references)
    for panel in panels:
        ref = references[panel["panelRefName"]]
        assert ref["id"] in objects and objects[ref["id"]]["type"] == ref["type"]


def test_vega_specs_query_contract_indices_with_dashboard_context():
    for obj in saved_objects():
        if obj["type"] != "visualization":
            continue
        spec = json.loads(json.loads(obj["attributes"]["visState"])["params"]["spec"])
        url = spec["data"]["url"]
        assert url["%context%"] is True
        assert (url["index"], url["%timefield%"]) in {("vp-docs", "event_time"), ("vp-dlq", "detected_at")}


def test_dashboard_auto_refreshes_and_restores_time_range():
    attributes = _by_id()[DASHBOARD_ID]["attributes"]
    assert attributes["timeRestore"] is True
    assert attributes["refreshInterval"] == {"pause": False, "value": 30000}
