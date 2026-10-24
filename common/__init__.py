"""Logic dùng chung cho crawler, speed layer và batch layer."""

from .text_processing import analyze_sentiment, normalize_text

__all__ = ["analyze_sentiment", "normalize_text"]
