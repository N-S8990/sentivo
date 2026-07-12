"""Abstract base for all data collectors."""

from abc import ABC, abstractmethod
from typing import Any


class BaseCollector(ABC):
    """Every data source collector inherits from this."""

    @abstractmethod
    def fetch_data(self, *args, **kwargs) -> Any:
        """Pull raw data from the source."""
        pass
