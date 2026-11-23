"""Sentiment analysis: the seam where a classifier or model call would go."""

from __future__ import annotations

from typing import Protocol

SENTIMENT_NEGATIVE = "negative"
SENTIMENT_NEUTRAL = "neutral"
SENTIMENT_POSITIVE = "positive"


class SentimentAnalyzer(Protocol):
    def analyze_sentiment(self, text: str) -> str: ...


class MockSentimentAnalyzer:
    """Deterministic local stand-in for a model: keyword counts, no network."""

    NEGATIVE_WORDS = ("not", "late", "never", "broken", "angry", "refund", "wrong", "missing")
    POSITIVE_WORDS = ("thanks", "thank", "great", "good", "perfect", "love", "resolved")

    def analyze_sentiment(self, text: str) -> str:
        words = [word.strip(".,!?;:'\"").lower() for word in text.split()]
        score = sum(word in self.POSITIVE_WORDS for word in words) - sum(
            word in self.NEGATIVE_WORDS for word in words
        )
        if score < 0:
            return SENTIMENT_NEGATIVE
        if score > 0:
            return SENTIMENT_POSITIVE
        return SENTIMENT_NEUTRAL
