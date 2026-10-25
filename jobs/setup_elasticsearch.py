"""Mapping tài liệu và alias DLQ có ILM 3 ngày cho M2.3.

Chạy trước khi submit job: python -m jobs.setup_elasticsearch --url http://localhost:9200
"""

import argparse
import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen

DOCS_MAPPING = {
    "dynamic": False,
    "properties": {
        **{
            name: {"type": "keyword"}
            for name in ("document_id", "record_id", "source", "site", "category", "article_id", "thread_id")
        },
        **{name: {"type": "date"} for name in ("event_time", "ingest_time")},
        **{
            name: {"type": "text"}
            for name in ("title", "summary", "body", "content", "thread_title", "normalized_text")
        },
        "url": {"type": "keyword", "index": False},
        "schema_version": {"type": "long"},
        "payload_json": {"type": "text", "index": False},
        "sentiment": {
            "properties": {
                "label": {"type": "keyword"},
                "score": {"type": "double"},
                "model": {"type": "keyword"},
            }
        },
    },
}
DLQ_MAPPING = {
    "dynamic": False,
    "properties": {
        **{name: {"type": "keyword"} for name in ("document_id", "error", "source_topic")},
        **{name: {"type": "date"} for name in ("kafka_timestamp", "detected_at")},
        "partition": {"type": "integer"},
        "offset": {"type": "long"},
        "raw_value": {"type": "text", "index": False},
    },
}


def request_json(url: str, path: str, body=None, method: str = "GET"):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = Request(
        url.rstrip("/") + path, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    with urlopen(request, timeout=30) as response:
        return json.load(response)


def setup_elasticsearch(url: str, docs_index: str = "vp-docs", dlq_index: str = "vp-dlq") -> None:
    """Chạy lại an toàn; mapping không tương thích phải báo lỗi để người vận hành xử lý."""
    try:
        request_json(url, f"/{docs_index}", {"mappings": DOCS_MAPPING}, "PUT")
    except HTTPError as error:
        details = json.loads(error.read())
        if error.code != 400 or details.get("error", {}).get("type") != "resource_already_exists_exception":
            raise RuntimeError(f"Không tạo được {docs_index}: {details}") from error
        request_json(url, f"/{docs_index}/_mapping", DOCS_MAPPING, "PUT")
    policy = f"{dlq_index}-retention"
    request_json(
        url,
        f"/_ilm/policy/{policy}",
        {"policy": {"phases": {"delete": {"min_age": "3d", "actions": {"delete": {}}}}}},
        "PUT",
    )
    request_json(
        url,
        f"/_index_template/{dlq_index}",
        {
            "index_patterns": [f"{dlq_index}-*"],
            "template": {
                "settings": {"index.lifecycle.name": policy, "number_of_shards": 1},
                "mappings": DLQ_MAPPING,
                "aliases": {dlq_index: {}},
            },
        },
        "PUT",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:9200")
    parser.add_argument("--docs-index", default="vp-docs")
    parser.add_argument("--dlq-index", default="vp-dlq")
    args = parser.parse_args()
    setup_elasticsearch(args.url, args.docs_index, args.dlq_index)
    print(f"Elasticsearch sẵn sàng: {args.docs_index}, {args.dlq_index} (alias, ILM 3 ngày)")


if __name__ == "__main__":
    main()
