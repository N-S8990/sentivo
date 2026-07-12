"""Market data producer — polls yfinance every 15 seconds."""

import logging
import time
from typing import Any, Dict

import pandas as pd
import yfinance as yf
from kafka import KafkaProducer

from sentivo.core.kafka_client import get_kafka_producer
from sentivo.producers.base import BaseProducer
from sentivo.schemas import RawMarketMessage

logger = logging.getLogger(__name__)


class MarketDataProducer(BaseProducer):
    """Polls yfinance for the latest price & volume and publishes to 'raw_market_data'."""

    POLL_INTERVAL = 15

    def __init__(self, config: Dict[str, Any]):
        logger.info("Initialising MarketDataProducer ...")
        self.producer: KafkaProducer = get_kafka_producer()
        self.assets = config.get("assets", [])
        self.topic = "raw_market_data"
        self.tickers = list({a["ticker"] for a in self.assets if "ticker" in a})
        if not self.tickers:
            logger.warning("No tickers configured.")
        else:
            logger.info("Tracking: %s", ", ".join(self.tickers))
            self.yf_tickers = yf.Tickers(" ".join(self.tickers))

    def run(self):
        if not self.tickers:
            return
        logger.info("Starting MarketDataProducer ...")
        while True:
            try:
                start = time.time()
                hist = self.yf_tickers.history(period="1d", interval="1m")
                if hist.empty:
                    time.sleep(self.POLL_INTERVAL)
                    continue

                for ticker in self.tickers:
                    try:
                        close = hist["Close"][ticker].iloc[-1]
                        volume = hist["Volume"][ticker].iloc[-1]
                        if pd.isna(close) or pd.isna(volume):
                            continue
                        ts = hist.index[-1].timestamp()
                        msg = RawMarketMessage(
                            ticker=ticker,
                            timestamp_utc=ts,
                            price=float(close),
                            volume=int(volume),
                        )
                        self.producer.send(self.topic, value=msg.model_dump())
                        logger.info("MARKET | %s | %.2f", ticker, close)
                    except Exception as e:
                        logger.error("Ticker %s error: %s", ticker, e)

                elapsed = time.time() - start
                time.sleep(max(0, self.POLL_INTERVAL - elapsed))
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error("Market loop error: %s — retry in 60s", e)
                time.sleep(60)

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
