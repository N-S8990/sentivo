"""Aggregator consumer — merges sentiment + market data into a Fear & Greed Index."""

import logging
import time
from collections import deque
from typing import Any, Deque, Dict, Optional, Tuple

from kafka import KafkaConsumer, KafkaProducer

from sentivo.consumers.base import BaseConsumer
from sentivo.core.kafka_client import get_kafka_consumer, get_kafka_producer
from sentivo.schemas import (
    AggregatedMetricsMessage,
    AnalyzedSentimentMessage,
    RawMarketMessage,
)

logger = logging.getLogger(__name__)


class AssetAggregator:
    """Maintains rolling windows of sentiment and price data for one asset."""

    def __init__(self, asset_name: str, ticker: str):
        self.asset_name = asset_name
        self.ticker = ticker
        self.sentiment_window: Deque[Tuple[float, float]] = deque()
        self.price_window: Deque[Tuple[float, float]] = deque()
        self.current_price = 0.0
        self.current_volume = 0.0
        self.last_1min_sent = 0.0

    def add_sentiment(self, ts: float, score: float):
        self.sentiment_window.append((ts, score))

    def add_market_data(self, ts: float, price: float, volume: float):
        self.price_window.append((ts, price))
        self.current_price = price
        self.current_volume = volume

    def _prune(self, window: Deque, max_age: int):
        now = time.time()
        while window and window[0][0] < (now - max_age):
            window.popleft()

    def aggregate(self) -> AggregatedMetricsMessage:
        now = time.time()
        self._prune(self.sentiment_window, 900)
        self._prune(self.price_window, 900)

        def window(w, secs):
            return deque(x for x in w if x[0] >= (now - secs))

        s1 = self._avg(window(self.sentiment_window, 60))
        s5 = self._avg(window(self.sentiment_window, 300))
        s15 = self._avg(self.sentiment_window)

        p1 = self._pct_chg(window(self.price_window, 60))
        p5 = self._pct_chg(window(self.price_window, 300))

        velocity = None
        if s1 is not None:
            if self.last_1min_sent != 0.0:
                velocity = s1 - self.last_1min_sent
            self.last_1min_sent = s1

        # Fear & Greed components
        sent_score = (s5 + 1) * 50 if s5 is not None else 50.0
        mom_score = (
            (max(-5, min(5, p5)) + 5) * 10 if p5 is not None else 50.0
        )
        vel_score = (
            (max(-0.5, min(0.5, velocity)) + 0.5) * 100
            if velocity is not None else 50.0
        )
        fg = sent_score * 0.5 + mom_score * 0.3 + vel_score * 0.2

        return AggregatedMetricsMessage(
            asset_name=self.asset_name,
            ticker=self.ticker,
            timestamp_utc=now,
            sentiment_1min_avg=s1,
            sentiment_5min_avg=s5,
            sentiment_15min_avg=s15,
            sentiment_velocity=velocity,
            price=self.current_price,
            volume=self.current_volume,
            price_change_1min_pct=p1,
            price_change_5min_pct=p5,
            fear_greed_score=fg,
        )

    @staticmethod
    def _avg(w: Deque) -> Optional[float]:
        return sum(x[1] for x in w) / len(w) if w else None

    @staticmethod
    def _pct_chg(w: Deque) -> Optional[float]:
        if len(w) < 2:
            return None
        return ((w[-1][1] - w[0][1]) / w[0][1]) * 100 if w[0][1] else None


class AggregatorConsumer(BaseConsumer):
    """Subscribes to analyzed_sentiment + raw_market_data and publishes
    aggregated_metrics (Fear & Greed) every 5 seconds."""

    PUBLISH_EVERY = 5

    def __init__(self, config: Dict[str, Any]):
        logger.info("Initialising AggregatorConsumer ...")
        self.consumer = get_kafka_consumer(
            topic=["analyzed_sentiment", "raw_market_data"],
            group_id="aggregators",
        )
        self.producer = get_kafka_producer()
        self.assets = config.get("assets", [])
        self.out_topic = "aggregated_metrics"

        self.aggregators: Dict[str, AssetAggregator] = {}
        self.ticker_to_asset: Dict[str, str] = {}
        for a in self.assets:
            self.ticker_to_asset[a["ticker"]] = a["name"]
            self.aggregators[a["name"]] = AssetAggregator(a["name"], a["ticker"])

        self.last_pub = 0.0

    def run(self):
        logger.info("Starting AggregatorConsumer ...")
        try:
            for msg in self.consumer:
                now = time.time()
                try:
                    if msg.topic == "analyzed_sentiment":
                        data = AnalyzedSentimentMessage.model_validate(msg.value)
                        if data.asset_name in self.aggregators:
                            self.aggregators[data.asset_name].add_sentiment(
                                data.timestamp_utc, data.sentiment_score
                            )
                    elif msg.topic == "raw_market_data":
                        data = RawMarketMessage.model_validate(msg.value)
                        name = self.ticker_to_asset.get(data.ticker)
                        if name and name in self.aggregators:
                            self.aggregators[name].add_market_data(
                                data.timestamp_utc, data.price, data.volume
                            )

                    if (now - self.last_pub) > self.PUBLISH_EVERY:
                        self.last_pub = now
                        for name, agg in self.aggregators.items():
                            m = agg.aggregate()
                            self.producer.send(self.out_topic, value=m.model_dump())
                            logger.info(
                                "AGGREGATOR | %-8s | F&G: %.2f",
                                name, m.fear_greed_score,
                            )
                except Exception as e:
                    logger.error("Aggregator error: %s", e, exc_info=True)
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
