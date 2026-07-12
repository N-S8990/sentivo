"""Abstract bases for producers and consumers."""

from abc import ABC, abstractmethod
from typing import Any


class BaseProducer(ABC):
    """Every Kafka producer inherits from this."""

    @abstractmethod
    def run(self, *args, **kwargs) -> Any:
        """Start producing messages."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Shut down the producer cleanly."""
        pass


class BaseConsumer(ABC):
    """Every Kafka consumer inherits from this."""

    @abstractmethod
    def run(self, *args, **kwargs) -> Any:
        """Start consuming and processing messages."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Shut down the consumer cleanly."""
        pass
