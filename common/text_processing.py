import re
import unicodedata

# Các hằng số từ điển baseline
POSITIVE_WORDS = {"tốt", "tuyệt vời", "hài lòng", "hay"}
NEGATIVE_WORDS = {"tệ", "kém", "dở", "lỗi"}
NEGATIONS = {"không", "chẳng", "chưa"}
INTENSIFIERS = {"rất", "cực kỳ", "quá"}
TOKEN_PATTERN = re.compile(r'\w+')

def normalize_text(text: str | None) -> str:
    """Chuẩn hóa văn bản: xử lý None, xóa HTML tags, entities, chuẩn hóa NFC và khoảng trắng."""
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', '', text)
    text = text.replace('&nbsp;', ' ')
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def _contains_phrase(text: str, phrase: str) -> bool:
    """Kiểm tra một cụm từ có tồn tại độc lập trong văn bản hay không."""
    pattern = r"(?<!\w)" + re.escape(phrase).replace(r"\ ", r"\s+") + r"(?!\w)"
    return bool(re.search(pattern, text, re.IGNORECASE))

def analyze_sentiment(text: str) -> dict:
    """Phân tích cảm xúc bằng luật cơ bản kết hợp từ điển và từ bổ nghĩa/phủ định."""
    words = TOKEN_PATTERN.findall(text)
    if not words:
        return {"label": "neutral", "score": 0.0}

    positive_hits = [phrase for phrase in POSITIVE_WORDS if _contains_phrase(text, phrase)]
    negative_hits = [phrase for phrase in NEGATIVE_WORDS if _contains_phrase(text, phrase)]

    score = 0.0
    matched_spans: list[tuple[int, int]] = []

    for phrase, polarity in ([(p, 1) for p in positive_hits] + [(p, -1) for p in negative_hits]):
        pattern = r"(?<!\w)" + re.escape(phrase).replace(r"\ ", r"\s+") + r"(?!\w)"
        for match in re.finditer(pattern, text, re.IGNORECASE):
            span = match.span()
            if any(span[0] < end and start < span[1] for start, end in matched_spans):
                continue
            matched_spans.append(span)

            before = text[max(0, span[0] - 30):span[0]]
            negated = any(_contains_phrase(before, negation) for negation in NEGATIONS)
            intensified = any(_contains_phrase(before, word) for word in INTENSIFIERS)

            weight = 2.0 if intensified else 1.0
            score += -polarity * weight if negated else polarity * weight

    label = "positive" if score > 0 else "negative" if score < 0 else "neutral"
    return {"label": label, "score": score}