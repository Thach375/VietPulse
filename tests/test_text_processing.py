import unittest

from common.text_processing import analyze_sentiment, normalize_text


class TestTextProcessing(unittest.TestCase):
    def test_normalize_html(self):
        self.assertEqual(normalize_text("<p>Xin chào&nbsp; mọi người</p>"), "Xin chào mọi người")

    def test_normalize_none(self):
        self.assertEqual(normalize_text(None), "")

    def test_normalize_whitespace(self):
        self.assertEqual(normalize_text("  Xin   chào \n Việt Nam  "), "Xin chào Việt Nam")

    def test_positive_sentiment(self):
        self.assertEqual(analyze_sentiment("Dịch vụ rất tốt!")["label"], "positive")

    def test_negative_sentiment(self):
        self.assertEqual(analyze_sentiment("Chất lượng rất tệ!")["label"], "negative")

    def test_negation(self):
        self.assertEqual(analyze_sentiment("Sản phẩm không tốt")["label"], "negative")

    def test_empty_text(self):
        result = analyze_sentiment("")
        self.assertEqual(result["label"], "neutral")
        self.assertEqual(result["score"], 0.0)

    def test_neutral_text(self):
        self.assertEqual(analyze_sentiment("Hôm nay là thứ hai")["label"], "neutral")

    def test_none_text_is_neutral_instead_of_error(self):
        self.assertEqual(analyze_sentiment(None), {"label": "neutral", "score": 0.0})

    def test_normalize_decodes_html_entities(self):
        self.assertEqual(normalize_text("Rất tốt &amp; hữu ích &quot;thật&quot;"), 'Rất tốt & hữu ích "thật"')

    def test_normalize_expands_teencode_words(self):
        self.assertEqual(normalize_text("Ko đc đâu mn ơi"), "không được đâu mọi người ơi")

    def test_normalize_keeps_amounts_with_k(self):
        self.assertEqual(normalize_text("Giá 10k, còn kia 50 k"), "Giá 10k, còn kia 50 k")

    def test_normalize_collapses_repeated_letters_and_emoji_but_not_digits(self):
        self.assertEqual(normalize_text("Quááá đỉnh 😍😍😍 giá 1000"), "Quá đỉnh 😍 giá 1000")

    def test_teencode_negation(self):
        self.assertEqual(analyze_sentiment("ko ổn chút nào")["label"], "negative")

    def test_emoji_counts_toward_sentiment(self):
        self.assertEqual(analyze_sentiment("😍😍")["label"], "positive")
        self.assertEqual(analyze_sentiment("đọc xong 😡")["label"], "negative")


if __name__ == "__main__":
    unittest.main()
