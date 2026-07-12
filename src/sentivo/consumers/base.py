"""Base consumer ABC — shared across all Kafka consumer implementations."""

from abc import ABC, abstractmethod
from typing import Any


class BaseConsumer(ABC):
    @abstractmethod
    def run(self, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    def close(self) -> None:
        pass
