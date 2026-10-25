from datetime import UTC, datetime, timedelta

from crawlers.run import CrawlState, next_poll_time, publish_new, topic_for

NOW = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)


def _env(source, record_id):
    return {
        "source": source,
        "record_id": record_id,
        "event_time": "2026-10-25T11:00:00Z",
        "ingest_time": "2026-10-25T12:00:00Z",
        "schema_version": 1,
        "payload": {},
    }


class FakePublisher:
    def __init__(self, reject=()):
        self.sent = []
        self.reject = set(reject)

    def publish(self, envelopes):
        delivered = []
        for envelope in envelopes:
            key = f"{envelope['source']}:{envelope['record_id']}"
            self.sent.append((topic_for(envelope), key))
            if key not in self.reject:
                delivered.append(key)
        return delivered


def test_topics_follow_data_contract():
    assert topic_for(_env("news_article", "vnexpress:1")) == "raw.news.articles"
    assert topic_for(_env("news_comment", "tuoitre:a")) == "raw.news.comments"
    assert topic_for(_env("voz_post", "voz:1")) == "raw.forum.posts"


def test_next_poll_time_follows_schedule():
    assert next_poll_time(NOW, NOW) == NOW + timedelta(minutes=15)
    assert next_poll_time(NOW, NOW + timedelta(hours=6, minutes=5)) == NOW + timedelta(hours=7)
    assert next_poll_time(NOW, NOW + timedelta(hours=72)) is None


def test_only_unsent_records_are_published_and_marked_after_delivery():
    state = CrawlState(":memory:")
    batch = [
        _env("news_comment", "tuoitre:a"),
        _env("news_comment", "tuoitre:b"),
        _env("news_comment", "tuoitre:a"),
    ]
    first = FakePublisher(reject={"news_comment:tuoitre:b"})
    assert publish_new(state, first, batch, NOW) == 1
    assert [key for _, key in first.sent] == ["news_comment:tuoitre:a", "news_comment:tuoitre:b"]

    # Lượt sau: a đã gửi nên bỏ qua; b chưa được Kafka xác nhận nên được gửi lại.
    second = FakePublisher()
    assert publish_new(state, second, batch, NOW) == 1
    assert second.sent == [("raw.news.comments", "news_comment:tuoitre:b")]


def test_sent_keys_survive_restart(tmp_path):
    path = tmp_path / "state" / "crawler.sqlite"
    publish_new(CrawlState(path), FakePublisher(), [_env("voz_post", "voz:1")], NOW)
    assert CrawlState(path).unsent([_env("voz_post", "voz:1")]) == []


def test_new_article_is_polled_now_then_follows_schedule_until_last_poll():
    state = CrawlState(":memory:")
    published = NOW - timedelta(minutes=50)
    state.track_article("vnexpress", "1", "https://vnexpress.net/a-1.html", published, NOW)

    [article] = state.due_articles(NOW, limit=10)
    state.reschedule(article, NOW)
    assert state.due_articles(NOW, limit=10) == []
    assert state.due_articles(published + timedelta(minutes=60), limit=10) == [article]

    state.reschedule(article, published + timedelta(hours=72))
    assert state.due_articles(published + timedelta(days=10), limit=10) == []


def test_articles_older_than_last_poll_are_not_tracked():
    state = CrawlState(":memory:")
    state.track_article("tuoitre", "x", "https://tuoitre.vn/x.htm", NOW - timedelta(days=4), NOW)
    assert state.due_articles(NOW, limit=10) == []
