"""Signal generation consumer — turns Fear & Greed scores into trade signals."""

import logging
import time
from typing import Dict

from kafka import KafkaConsumer, KafkaProducer

from sentivo.consumers.base import BaseConsumer
from sentivo.core.kafka_client import get_kafka_consumer, get_kafka_producer
from sentivo.schemas import AggregatedMetricsMessage, TradeSignalMessage

logger = logging.getLogger(__name__)


class SignalConsumer(BaseConsumer):
    """Three strategies: contrarian buy, contrarian sell, trend-follow buy."""

    COOLDOWN = 300

    def __init__(self):
        logger.info("Initialising SignalConsumer ...")
        self.consumer = get_kafka_consumer(
            topic="aggregated_metrics",
            group_id="signal-generators",
            auto_offset_reset="latest",
        )
        self.producer = get_kafka_producer()
        self.out_topic = "trade_signals"
        self.last_signal: Dict[str, float] = {}

    def run(self):
        logger.info("Starting SignalConsumer ...")
        try:
            for msg in self.consumer:
                try:
                    data = AggregatedMetricsMessage.model_validate(msg.value)
                    now = time.time()

                    signal = "HOLD"
                    confidence = 0.0
                    reason = "Neutral — no trigger met"

                    # Strategy 1: Contrarian Buy (extreme fear + recovering sentiment)
                    if (
                        data.fear_greed_score < 20
                        and data.sentiment_velocity is not None
                        and data.sentiment_velocity > 0.1
                    ):
                        signal = "BUY"
                        confidence = 0.75
                        reason = "Contrarian buy — extreme fear with sentiment recovery"

                    # Strategy 2: Contrarian Sell (extreme greed + price dip)
                    elif (
                        data.fear_greed_score > 85
                        and data.price_change_1min_pct is not None
                        and data.price_change_1min_pct < -0.2
                    ):
                        signal = "SELL"
                        confidence = 0.80
                        reason = "Contrarian sell — extreme greed with negative momentum"

                    # Strategy 3: Trend Follow (strong greed + price surging)
                    elif (
                        data.fear_greed_score > 70
                        and data.sentiment_5min_avg is not None
                        and data.sentiment_5min_avg > 0.3
                        and data.price_change_5min_pct is not None
                        and data.price_change_5min_pct > 0.5
                    ):
                        signal = "BUY"
                        confidence = 0.60
                        reason = "Trend follow — strong greed, positive sentiment & price"

                    out = TradeSignalMessage(
                        asset_name=data.asset_name,
                        ticker=data.ticker,
                        timestamp_utc=now,
                        signal=signal,
                        confidence=confidence,
                        reason=reason,
                        fear_greed_score=data.fear_greed_score,
                    )
                    self.producer.send(self.out_topic, value=out.model_dump())

                    if signal != "HOLD":
                        self.last_signal[data.asset_name] = now
                        icon = "🟢" if signal == "BUY" else "🔴"
                        logger.warning(
                            "%s SIGNAL | %-8s | %s | %.0f%% | F&G: %.1f | %s",
                            icon, data.asset_name, signal, confidence * 100,
                            data.fear_greed_score, reason,
                        )
                    else:
                        logger.info(
                            "⚪ SIGNAL | %-8s | HOLD | F&G: %.1f | Sent: %.3f",
                            data.asset_name, data.fear_greed_score,
                            data.sentiment_5min_avg or 0.0,
                        )
                except Exception as e:
                    logger.error("Signal error: %s", e, exc_info=True)
        except KeyboardInterrupt:
            pass
        finally:
            self.close()

    def close(self):
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
            self.producer.close()
