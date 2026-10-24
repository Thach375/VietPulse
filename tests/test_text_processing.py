import unittest
from common.text_processing import normalize_text, analyze_sentiment

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

if __name__ == "__main__":
    unittest.main()