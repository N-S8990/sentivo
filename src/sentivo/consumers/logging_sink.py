"""Logging sink — pretty-prints the final trade signals to stdout."""

import logging
from datetime import datetime

from kafka import KafkaConsumer

from sentivo.consumers.base import BaseConsumer
from sentivo.core.kafka_client import get_kafka_consumer
from sentivo.schemas import TradeSignalMessage

logger = logging.getLogger(__name__)


class LoggingSink(BaseConsumer):
    """Terminal sink for trade_signals. Subscribes and logs every signal."""

    def __init__(self):
        logger.info("Initialising LoggingSink ...")
        self.consumer = get_kafka_consumer(
            topic="trade_signals", group_id="log-sinks"
        )

    def run(self):
        logger.info("Listening on trade_signals ...")
        try:
            for msg in self.consumer:
                try:
                    data = TradeSignalMessage.model_validate(msg.value)
                    ts = datetime.fromtimestamp(data.timestamp_utc).isoformat()
                    logger.warning("=" * 70)
                    logger.warning(
                        "  FINAL SIGNAL | %s (%s)", data.asset_name, data.ticker
                    )
                    logger.warning("  %s", ts)
                    logger.warning(
                        "  %s (%.0f%%)", data.signal, data.confidence * 100
                    )
                    logger.warning("  %s", data.reason)
                    logger.warning("  F&G Score: %.2f", data.fear_greed_score)
                    logger.warning("=" * 70)
                except Exception as e:
                    logger.error("Logging error: %s", e, exc_info=True)
        except KeyboardInterrupt:
            pass
        finally:
            self.close()

    def close(self):
        if self.consumer:
            self.consumer.close()
