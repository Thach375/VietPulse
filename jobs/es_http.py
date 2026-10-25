"""Elasticsearch 8 HTTPS client for small administrative calls and bulk indexing.

Credentials and the ECK CA certificate are read from mounted files, never URLs
or Spark configuration. Bulk actions use stable IDs and check per-item failures.
https://www.elastic.co/guide/en/elasticsearch/reference/8.19/docs-bulk.html
"""

import base64
import json
import os
import ssl
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


def _json_default(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=value.tzinfo or UTC).astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Unsupported Elasticsearch field type: {type(value).__name__}")


def request_json(url: str, path: str, body=None, method: str = "GET", content_type="application/json"):
    data = (
        body
        if isinstance(body, bytes)
        else json.dumps(body, default=_json_default).encode()
        if body is not None
        else None
    )
    headers = {"Content-Type": content_type}
    password_file = os.environ.get("VP_ES_PASSWORD_FILE")
    target = urlsplit(url)
    if target.username or target.password or target.path not in ("", "/") or target.query or target.fragment:
        raise ValueError("Elasticsearch URL must contain only scheme, host and port")
    if password_file:
        if target.scheme != "https":
            raise ValueError("Elasticsearch credentials require HTTPS")
        password = Path(password_file).read_text()
        username = os.environ.get("VP_ES_USERNAME", "elastic")
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    if url.startswith("https://"):
        ca_file = os.environ.get("VP_ES_CA_FILE")
        if not ca_file:
            raise ValueError("HTTPS Elasticsearch requires VP_ES_CA_FILE")
        context = ssl.create_default_context(cafile=ca_file)
    elif url.startswith("http://"):
        context = None
    else:
        raise ValueError("Elasticsearch URL must use HTTP or HTTPS")
    request = Request(url.rstrip("/") + path, data=data, method=method, headers=headers)
    with urlopen(request, context=context, timeout=45) as response:
        return json.load(response)


def bulk_payload(documents: list[dict], index: str, index_field: str | None = None) -> bytes:
    lines = []
    for original in documents:
        document = original.copy()
        document_id = document.get("document_id")
        if not document_id:
            raise ValueError("Elasticsearch document_id is required")
        target = f"{index}-{document.pop(index_field)}" if index_field else index
        lines.append(json.dumps({"index": {"_index": target, "_id": document_id}}))
        lines.append(json.dumps(document, ensure_ascii=False, default=_json_default))
    return ("\n".join(lines) + "\n").encode() if lines else b""


def bulk_index_partition(rows, url: str, index: str, index_field: str | None = None):
    batch = []

    def flush():
        payload = bulk_payload(batch, index, index_field)
        result = request_json(url, "/_bulk", payload, "POST", "application/x-ndjson")
        if result.get("errors"):
            failed = [
                item["index"].get("status")
                for item in result.get("items", [])
                if item.get("index", {}).get("status", 500) >= 300
            ]
            raise RuntimeError(
                f"Elasticsearch bulk indexing failed for {len(failed)} documents: {failed[:5]}"
            )
        batch.clear()

    for row in rows:
        batch.append(row.asDict(recursive=True))
        if len(batch) == 500:
            flush()
    if batch:
        flush()
