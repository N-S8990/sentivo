"""Base class for sentiment analyzers."""

from abc import ABC, abstractmethod
from typing import Any


class BaseSentimentAnalyzer(ABC):
    """Every sentiment model wraps this interface."""

    @abstractmethod
    def predict(self, *args, **kwargs) -> Any:
        """Run inference on input text(s)."""
        pass
