"""Text normalisation pipeline for social and news text."""

import logging
import re
from functools import lru_cache

logger = logging.getLogger(__name__)


class TextPreprocessor:
    """Strips URLs, mentions, tickers, and normalises whitespace.
    
    Results are cached via LRU to avoid re-processing repeated phrases.
    """

    def __init__(self):
        self.url_pattern = re.compile(r"https?://\S+|www\.\S+")
        self.mention_pattern = re.compile(r"@\w+")
        self.ticker_pattern = re.compile(r"\$\w+")
        self.whitespace_pattern = re.compile(r"\s+")
        logger.info("TextPreprocessor initialised.")

    @lru_cache(maxsize=10000)
    def preprocess(self, text: str) -> str:
        """Normalise a single text string."""
        if not isinstance(text, str) or not text:
            return ""

        text = text.lower()
        text = self.url_pattern.sub("", text)
        text = self.mention_pattern.sub("", text)
        text = self.ticker_pattern.sub("", text)
        text = self.whitespace_pattern.sub(" ", text).strip()
        return text
