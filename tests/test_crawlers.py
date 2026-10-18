from crawlers.base import hash_user, parse_rss
from crawlers.news import VN_TZ, parse_article_page, parse_tuoitre_comments, parse_vnexpress_comments
from crawlers.voz import ThreadRef, parse_forum_page, parse_thread_page

RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <title>Tiêu đề A</title>
    <link>https://vnexpress.net/tieu-de-a-5133904.html</link>
    <description><![CDATA[<a href="x"><img src="y"/></a>Tóm tắt <b>A</b>]]></description>
    <pubDate>Sun, 18 Oct 2026 20:15:24 +0700</pubDate>
  </item>
</channel></rss>"""


def test_parse_rss_reads_items_with_timezone():
    [item] = parse_rss(RSS)
    assert item.title == "Tiêu đề A"
    assert item.summary == "Tóm tắt A"
    assert item.published.utcoffset().total_seconds() == 7 * 3600


def test_parse_rss_from_bytes_keeps_vietnamese_and_reads_us_dates():
    xml = (
        "<?xml version='1.0' encoding='UTF-8'?><rss><channel><item><title>Việt Nam</title>"
        "<link>https://tuoitre.vn/a-100261018182153684.htm</link>"
        "<pubDate>10/18/2026 8:21:00 PM</pubDate></item></channel></rss>"
    ).encode()
    [item] = parse_rss(xml, default_tz=VN_TZ)
    assert item.title == "Việt Nam"
    assert item.published.isoformat() == "2026-10-18T20:21:00+07:00"


def test_parse_rss_drops_naive_dates_without_default_tz():
    xml = (
        "<rss><channel><item><title>A</title><pubDate>10/18/2026 8:21:00 PM</pubDate></item></channel></rss>"
    )
    assert parse_rss(xml)[0].published is None


def test_parse_article_page_reads_body_and_published_time():
    html = """<script>{"datePublished": "2026-10-18T20:15:24+07:00"}</script>
    <article class="fck_detail"><p class="Normal">Đoạn 1</p><p class="Normal">Đoạn 2</p></article>"""
    body, published = parse_article_page(html, "article.fck_detail p.Normal")
    assert body == "Đoạn 1\nĐoạn 2"
    assert published.isoformat() == "2026-10-18T20:15:24+07:00"


def test_parse_vnexpress_comments():
    data = {
        "data": {
            "items": [
                {
                    "comment_id": 64790614,
                    "parent_id": 64790614,
                    "content": "Dòng 1<br />Dòng 2",
                    "creation_time": 1792332290,
                    "userid": 1048630107,
                    "full_name": "Không được lưu",
                    "userlike": 3,
                }
            ]
        }
    }
    [c] = parse_vnexpress_comments(data, "5133904")
    assert c["record_id"] == "vnexpress:64790614"
    assert c["event_time"] == "2026-10-18T14:04:50Z"
    assert c["payload"]["content"] == "Dòng 1 Dòng 2"
    assert "Không được lưu" not in str(c)
    assert c["payload"]["author_hash"] == hash_user("vnexpress", "1048630107")


def test_parse_tuoitre_comments_flattens_replies_and_converts_vn_time():
    items = [
        {
            "id": "a",
            "parent_id": "0",
            "content": "Gốc",
            "user_id": "u1",
            "likes": "9",
            "created_date": "2026-10-16T11:05:23.4734128",
            "published_date": "2026-10-16T11:54:15.166967",
            "child_comments": [
                {"id": "b", "parent_id": "a", "content": "Trả lời", "created_date": "2026-10-16T12:00:00"}
            ],
        }
    ]
    root, reply = parse_tuoitre_comments(items, "100261016093123076")
    assert root["event_time"] == "2026-10-16T04:05:23.473412Z"
    assert root["payload"]["published_time"] == "2026-10-16T04:54:15.166967Z"
    assert root["payload"]["parent_id"] is None
    assert reply["payload"]["parent_id"] == "a"


FORUM_HTML = """
<div class="structItemContainer-group structItemContainer-group--sticky">
  <div class="structItem structItem--thread"><div class="structItem-title">
    <a href="/t/noi-quy.1110746/">Nội quy</a></div></div>
</div>
<div class="structItemContainer-group">
  <div class="structItem structItem--thread"><div class="structItem-title">
    <a href="/forums/-/prefix">Tin</a><a href="/t/tin-moi.1289885/">Tin mới</a></div></div>
</div>"""

THREAD_HTML = """
<article class="message message--post" data-author="thuyvan" data-content="post-37305761">
  <header class="message-attribution"><time class="u-dt" data-timestamp="1749553204"></time></header>
  <div class="bbWrapper"><blockquote>trích dẫn bài khác</blockquote>Nội dung chính</div>
</article>"""


def test_parse_forum_page_marks_sticky_threads():
    sticky, normal = parse_forum_page(FORUM_HTML)
    assert sticky.sticky and sticky.thread_id == "1110746"
    assert not normal.sticky and normal.title == "Tin mới"


def test_parse_thread_page_drops_quotes_and_hashes_author():
    thread = ThreadRef("1289885", "https://voz.vn/t/tin-moi.1289885/", "Tin mới", False)
    [post] = parse_thread_page(THREAD_HTML, thread, 33)
    assert post["record_id"] == "voz:37305761"
    assert post["event_time"] == "2025-06-10T11:00:04Z"
    assert post["payload"]["content"] == "Nội dung chính"
    assert "thuyvan" not in str(post)
