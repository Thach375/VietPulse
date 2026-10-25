"""Tạo data view và dashboard Kibana cho real-time view (Task 11, M2.4).

Chạy sau setup_elasticsearch, khi Kibana đã lên:
    python -m jobs.setup_kibana --url http://localhost:5601                      # từ máy host
    docker compose exec spark python -m jobs.setup_kibana --url http://kibana:5601
Chạy lại an toàn: saved object có id cố định và được ghi đè.

Biểu đồ dùng Vega-Lite đọc thẳng vp-docs và vp-dlq, nên tự áp khoảng thời gian và bộ lọc của dashboard.
Dashboard tự làm mới mỗi 30 giây; mặc định xem 24 giờ gần nhất, chia ô 5 phút như cửa sổ của rt_window.
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from typing import Any

DOCS_VIEW_ID = "vp-docs"
DLQ_VIEW_ID = "vp-dlq"
DASHBOARD_ID = "vp-realtime"
KIBANA_VERSION = "8.15.3"
VEGA_SCHEMA = "https://vega.github.io/schema/vega-lite/v5.json"
SENTIMENT_SCALE = {"domain": ["positive", "neutral", "negative"], "range": ["#2e7d32", "#9e9e9e", "#c62828"]}
EMPTY_SEARCH = json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})


def _es_data(index: str, timefield: str, aggs: dict[str, Any], prop: str) -> dict[str, Any]:
    """Nguồn dữ liệu Vega: truy vấn aggregation, áp time filter và bộ lọc của dashboard (%context%)."""
    return {
        "url": {
            "%context%": True,
            "%timefield%": timefield,
            "index": index,
            "body": {"size": 0, "aggs": aggs},
        },
        "format": {"property": prop},
    }


def _by_label(group_agg: dict[str, Any]) -> dict[str, Any]:
    return {**group_agg, "aggs": {"label": {"terms": {"field": "sentiment.label", "size": 3}}}}


_FLATTEN_LABEL = [
    {"flatten": ["label.buckets"], "as": ["bucket"]},
    {"calculate": "datum.bucket.key", "as": "label"},
    {"calculate": "datum.bucket.doc_count", "as": "count"},
]
_SENTIMENT_COLOR = {"field": "label", "type": "nominal", "title": "Cảm xúc", "scale": SENTIMENT_SCALE}

SPECS: dict[str, tuple[str, dict[str, Any]]] = {
    "vp-vis-timeline": (
        "Số tài liệu theo cảm xúc (ô 5 phút)",
        {
            "$schema": VEGA_SCHEMA,
            "data": _es_data(
                "vp-docs",
                "event_time",
                {"time": _by_label({"date_histogram": {"field": "event_time", "fixed_interval": "5m"}})},
                "aggregations.time.buckets",
            ),
            "transform": _FLATTEN_LABEL,
            "mark": {"type": "bar", "tooltip": True},
            "encoding": {
                "x": {"field": "key", "type": "temporal", "title": None},
                "y": {"field": "count", "type": "quantitative", "title": "Số tài liệu", "stack": "zero"},
                "color": _SENTIMENT_COLOR,
            },
        },
    ),
    "vp-vis-sentiment-share": (
        "Tỉ lệ cảm xúc",
        {
            "$schema": VEGA_SCHEMA,
            "data": _es_data(
                "vp-docs",
                "event_time",
                {"label": {"terms": {"field": "sentiment.label", "size": 3}}},
                "aggregations.label.buckets",
            ),
            "mark": {"type": "arc", "innerRadius": 50, "tooltip": True},
            "encoding": {
                "theta": {"field": "doc_count", "type": "quantitative"},
                "color": {"field": "key", "type": "nominal", "title": "Cảm xúc", "scale": SENTIMENT_SCALE},
            },
        },
    ),
    "vp-vis-by-site": (
        "Số tài liệu theo trang",
        {
            "$schema": VEGA_SCHEMA,
            "data": _es_data(
                "vp-docs",
                "event_time",
                {"site": _by_label({"terms": {"field": "site", "size": 20}})},
                "aggregations.site.buckets",
            ),
            "transform": _FLATTEN_LABEL,
            "mark": {"type": "bar", "tooltip": True},
            "encoding": {
                "y": {"field": "key", "type": "nominal", "title": None, "sort": "-x"},
                "x": {"field": "count", "type": "quantitative", "title": "Số tài liệu", "stack": "zero"},
                "color": _SENTIMENT_COLOR,
            },
        },
    ),
    "vp-vis-dlq": (
        "Bản ghi lỗi trong DLQ theo lý do",
        {
            "$schema": VEGA_SCHEMA,
            "data": _es_data(
                "vp-dlq",
                "detected_at",
                {"error": {"terms": {"field": "error", "size": 10}}},
                "aggregations.error.buckets",
            ),
            "mark": {"type": "bar", "tooltip": True, "color": "#c62828"},
            "encoding": {
                "y": {"field": "key", "type": "nominal", "title": None, "sort": "-x"},
                "x": {"field": "doc_count", "type": "quantitative", "title": "Số bản ghi"},
            },
        },
    ),
}

# (id, loại, x, y, rộng, cao) trên lưới 48 cột của dashboard.
PANELS = [
    ("vp-vis-timeline", "visualization", 0, 0, 32, 14),
    ("vp-vis-sentiment-share", "visualization", 32, 0, 16, 14),
    ("vp-vis-by-site", "visualization", 0, 14, 24, 14),
    ("vp-vis-dlq", "visualization", 24, 14, 24, 14),
    ("vp-latest-docs", "search", 0, 28, 48, 16),
]


def _data_view(view_id: str, timefield: str) -> dict[str, Any]:
    return {
        "type": "index-pattern",
        "id": view_id,
        "attributes": {"title": view_id, "name": view_id, "timeFieldName": timefield},
        "references": [],
    }


def _vega_visualization(vis_id: str, title: str, spec: dict[str, Any]) -> dict[str, Any]:
    vis_state = {
        "title": title,
        "type": "vega",
        "aggs": [],
        "params": {"spec": json.dumps(spec, ensure_ascii=False, indent=2)},
    }
    return {
        "type": "visualization",
        "id": vis_id,
        "attributes": {
            "title": title,
            "description": "",
            "visState": json.dumps(vis_state, ensure_ascii=False),
            "uiStateJSON": "{}",
            "kibanaSavedObjectMeta": {"searchSourceJSON": EMPTY_SEARCH},
        },
        "references": [],
    }


def _latest_docs_search() -> dict[str, Any]:
    source = {
        "query": {"query": "", "language": "kuery"},
        "filter": [],
        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
    }
    return {
        "type": "search",
        "id": "vp-latest-docs",
        "attributes": {
            "title": "Tài liệu mới nhất",
            "description": "",
            "columns": ["site", "source", "sentiment.label", "title", "content"],
            "sort": [["event_time", "desc"]],
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(source)},
        },
        "references": [
            {
                "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                "type": "index-pattern",
                "id": DOCS_VIEW_ID,
            }
        ],
    }


def _dashboard() -> dict[str, Any]:
    panels, references = [], []
    for index, (object_id, object_type, x, y, w, h) in enumerate(PANELS):
        ref_name = f"panel_{index}"
        panels.append(
            {
                "version": KIBANA_VERSION,
                "type": object_type,
                "gridData": {"x": x, "y": y, "w": w, "h": h, "i": str(index)},
                "panelIndex": str(index),
                "embeddableConfig": {},
                "panelRefName": ref_name,
            }
        )
        references.append({"name": ref_name, "type": object_type, "id": object_id})
    return {
        "type": "dashboard",
        "id": DASHBOARD_ID,
        "attributes": {
            "title": "VietPulse · Real-time",
            "description": "Speed layer: tài liệu đã chấm sentiment (vp-docs) và bản ghi lỗi (vp-dlq)",
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps({"useMargins": True, "syncColors": False, "hidePanelTitles": False}),
            "timeRestore": True,
            "timeFrom": "now-24h",
            "timeTo": "now",
            "refreshInterval": {"pause": False, "value": 30000},
            "kibanaSavedObjectMeta": {"searchSourceJSON": EMPTY_SEARCH},
        },
        "references": references,
    }


def saved_objects() -> list[dict[str, Any]]:
    objects = [_data_view(DOCS_VIEW_ID, "event_time"), _data_view(DLQ_VIEW_ID, "detected_at")]
    objects += [_vega_visualization(vis_id, title, spec) for vis_id, (title, spec) in SPECS.items()]
    objects += [_latest_docs_search(), _dashboard()]
    return objects


def _request(url: str, method: str = "GET", body: Any = None) -> Any:
    request = urllib.request.Request(
        url,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json", "kbn-xsrf": "true"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def wait_for_kibana(url: str, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            status = _request(f"{url}/api/status")
            if status.get("status", {}).get("overall", {}).get("level") == "available":
                return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        if time.monotonic() > deadline:
            raise TimeoutError(f"Kibana chưa sẵn sàng sau {timeout_s:.0f} giây: {url}")
        time.sleep(5)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--url", default="http://localhost:5601", help="địa chỉ Kibana")
    parser.add_argument("--wait", type=float, default=180, help="số giây tối đa chờ Kibana sẵn sàng")
    args = parser.parse_args(argv)
    url = args.url.rstrip("/")

    wait_for_kibana(url, args.wait)
    result = _request(f"{url}/api/saved_objects/_bulk_create?overwrite=true", "POST", saved_objects())
    errors = [(o["type"], o["id"], o["error"]) for o in result["saved_objects"] if "error" in o]
    if errors:
        raise SystemExit(f"Kibana từ chối saved object: {errors}")
    print(f"Dashboard sẵn sàng: {url}/app/dashboards#/view/{DASHBOARD_ID}")


if __name__ == "__main__":
    main()
