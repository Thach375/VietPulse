import json

import pytest

pytest.importorskip("pyspark")
from jobs.speed_sentiment import JobConfig, parse_args  # noqa: E402


def test_defaults_read_live_sources_only():
    config = parse_args([])
    assert config.topics == ("raw.news.articles", "raw.news.comments", "raw.forum.posts")
    assert config.starting_offsets == "earliest"
    assert config.es_url == "http://elasticsearch:9200"


def test_cli_accepts_separate_checkpoint_and_optional_source():
    config = parse_args(
        [
            "--topics",
            " raw.news.comments,raw.youtube.comments ",
            "--checkpoint-location",
            "/checkpoints/docs",
            "--available-now",
        ]
    )
    assert config.topics == ("raw.news.comments", "raw.youtube.comments")
    assert config.checkpoint_location == "/checkpoints/docs"
    assert config.available_now


@pytest.mark.parametrize("topics", [(), ("raw.backfill",), ("dlq.raw",), ("alerts.sentiment",)])
def test_forbids_feedback_loops_and_backfill(topics):
    with pytest.raises(ValueError):
        JobConfig(topics=topics)


@pytest.mark.parametrize(
    "url", ["localhost:9200", "ftp://host", "http://host/index", "http://user:secret@host"]
)
def test_invalid_elasticsearch_url_is_rejected(url):
    with pytest.raises(ValueError):
        JobConfig(es_url=url)


def test_https_endpoint_allowed():
    assert JobConfig(es_url="https://elasticsearch:9243").es_url == "https://elasticsearch:9243"


def test_setup_preserves_existing_index_and_configures_dlq_retention(monkeypatch):
    from io import BytesIO
    from urllib.error import HTTPError

    from jobs import setup_elasticsearch as module

    calls = []

    def request(url, path, body=None, method="GET"):
        calls.append((path, body, method))
        if path == "/vp-docs":
            error = {"error": {"type": "resource_already_exists_exception"}}
            raise HTTPError(url + path, 400, "exists", {}, BytesIO(json.dumps(error).encode()))
        return {}

    monkeypatch.setattr(module, "request_json", request)
    module.setup_elasticsearch("http://es:9200")
    assert calls[1][0] == "/vp-docs/_mapping"
    assert calls[2][1]["policy"]["phases"]["delete"]["min_age"] == "3d"
    template = calls[3][1]["template"]
    assert "vp-dlq" in template["aliases"]
    assert template["settings"]["index.lifecycle.name"] == "vp-dlq-retention"
