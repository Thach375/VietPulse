"""Chuẩn hóa văn bản và chấm sentiment tạm bằng luật cho speed layer (Task 8).

Bản luật chỉ dùng đến khi có PhoBERT (M4.2): từ điển nhỏ và không hiểu ngữ cảnh.
"""

import html
import re
import unicodedata

# Các hằng số từ điển baseline
POSITIVE_WORDS = {"tốt", "tuyệt vời", "hài lòng", "hay", "thích", "yêu", "đỉnh", "ổn", "xuất sắc", "cảm ơn"}
NEGATIVE_WORDS = {"tệ", "kém", "dở", "lỗi", "chán", "thất vọng", "buồn", "tồi", "xấu"}
NEGATIONS = {"không", "chẳng", "chưa", "chả"}
INTENSIFIERS = {"rất", "cực kỳ", "quá"}
TOKEN_PATTERN = re.compile(r"\w+")

# Teencode hay gặp trong bình luận; chỉ thay khi đứng thành một từ riêng.
TEENCODE = {
    "ko": "không",
    "k": "không",
    "kh": "không",
    "khg": "không",
    "hok": "không",
    "dc": "được",
    "đc": "được",
    "dk": "được",
    "đk": "được",
    "bt": "bình thường",
    "j": "gì",
    "vs": "với",
    "mn": "mọi người",
    "mng": "mọi người",
    "ng": "người",
    "cx": "cũng",
    "r": "rồi",
    "z": "vậy",
    "trc": "trước",
    "ntn": "như thế nào",
    "thik": "thích",
    "thix": "thích",
    "iu": "yêu",
    "bik": "biết",
    "lun": "luôn",
    "wá": "quá",
    "qá": "quá",
    "tks": "cảm ơn",
    "thks": "cảm ơn",
}
# Không thay sau "số + khoảng trắng" để giữ "50 k" (50 nghìn); "10k" vốn không khớp vì dính chữ số.
_TEENCODE_PATTERN = re.compile(
    r"(?<!\w)(?<!\d )("
    + "|".join(re.escape(w) for w in sorted(TEENCODE, key=len, reverse=True))
    + r")(?!\w)",
    re.IGNORECASE,
)
# Chữ hoặc emoji lặp từ 3 lần trở lên ("quááá", "😍😍😍") gộp còn một; không gộp chữ số ("1000").
_REPEATED_CHAR = re.compile(r"([^\d\s])\1{2,}")

EMOJI_SENTIMENT = {
    "😍": 1,
    "🥰": 1,
    "😊": 1,
    "😁": 1,
    "😄": 1,
    "👍": 1,
    "👏": 1,
    "❤": 1,
    "💯": 1,
    "🎉": 1,
    "😡": -1,
    "🤬": -1,
    "😠": -1,
    "😢": -1,
    "😭": -1,
    "😞": -1,
    "😒": -1,
    "👎": -1,
    "💔": -1,
}


def normalize_text(text: str | None) -> str:
    """Bỏ thẻ HTML, giải mã ký tự HTML, chuẩn hóa NFC, gộp ký tự lặp, đổi teencode, gộp khoảng trắng."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = unicodedata.normalize("NFC", text)
    text = text.replace("️", "")  # bỏ ký tự chọn kiểu emoji: "❤️" thành "❤"
    text = _REPEATED_CHAR.sub(r"\1", text)
    text = _TEENCODE_PATTERN.sub(lambda m: TEENCODE[m.group(1).lower()], text)
    return re.sub(r"\s+", " ", text).strip()


def _contains_phrase(text: str, phrase: str) -> bool:
    """Kiểm tra một cụm từ có tồn tại độc lập trong văn bản hay không."""
    pattern = r"(?<!\w)" + re.escape(phrase).replace(r"\ ", r"\s+") + r"(?!\w)"
    return bool(re.search(pattern, text, re.IGNORECASE))


def analyze_sentiment(text: str | None) -> dict:
    """Phân tích cảm xúc bằng từ điển, từ phủ định, từ nhấn mạnh và emoji.

    Tự chuẩn hóa văn bản trước khi chấm; None hoặc văn bản rỗng trả về neutral, không ném lỗi,
    để UDF của Spark không làm dừng job khi gặp bản ghi không có nội dung.
    """
    text = normalize_text(text)
    if not text:
        return {"label": "neutral", "score": 0.0}

    positive_hits = [phrase for phrase in POSITIVE_WORDS if _contains_phrase(text, phrase)]
    negative_hits = [phrase for phrase in NEGATIVE_WORDS if _contains_phrase(text, phrase)]

    score = 0.0
    matched_spans: list[tuple[int, int]] = []

    for phrase, polarity in [(p, 1) for p in positive_hits] + [(p, -1) for p in negative_hits]:
        pattern = r"(?<!\w)" + re.escape(phrase).replace(r"\ ", r"\s+") + r"(?!\w)"
        for match in re.finditer(pattern, text, re.IGNORECASE):
            span = match.span()
            if any(span[0] < end and start < span[1] for start, end in matched_spans):
                continue
            matched_spans.append(span)

            before = text[max(0, span[0] - 30) : span[0]]
            negated = any(_contains_phrase(before, negation) for negation in NEGATIONS)
            intensified = any(_contains_phrase(before, word) for word in INTENSIFIERS)

            weight = 2.0 if intensified else 1.0
            score += -polarity * weight if negated else polarity * weight

    score += sum(EMOJI_SENTIMENT.get(ch, 0) for ch in text)
    label = "positive" if score > 0 else "negative" if score < 0 else "neutral"
    return {"label": label, "score": score}
