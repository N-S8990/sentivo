"""News polling producer — fetches articles from NewsAPI on a timer."""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict

import requests
from kafka import KafkaProducer

from sentivo.core.kafka_client import get_kafka_producer
from sentivo.producers.base import BaseProducer
from sentivo.schemas import NewsApiResponse, RawTextMessage

logger = logging.getLogger(__name__)


class NewsProducer(BaseProducer):
    """Polls NewsAPI every 5 minutes and publishes new articles to Kafka."""

    BASE_URL = "https://newsapi.org/v2/everything"
    POLL_INTERVAL = 300  # 5 minutes

    def __init__(self, config: Dict[str, Any]):
        self.producer: KafkaProducer = get_kafka_producer()
        self.assets = config.get("assets", [])
        self.api_key = os.getenv("NEWSAPI_API_KEY")
        if not self.api_key:
            raise ValueError("NEWSAPI_API_KEY not set")
        self.topic = "raw_text_data"
        self.seen_urls: set[str] = set()

        self.queries = []
        for asset in self.assets:
            for q in asset.get("news_queries", []):
                self.queries.append({"asset": asset["name"], "query": q})

    def _fetch(self, asset_name: str, query: str):
        params = {
            "q": query,
            "apiKey": self.api_key,
            "pageSize": 20,
            "sortBy": "publishedAt",
            "language": "en",
        }
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = NewsApiResponse.model_validate(resp.json())

            for article in data.articles:
                if article.url in self.seen_urls:
                    continue
                self.seen_urls.add(article.url)

                ts = (
                    datetime.fromisoformat(article.publishedAt)
                    .replace(tzinfo=timezone.utc)
                    .timestamp()
                )
                msg = RawTextMessage(
                    asset_name=asset_name,
                    source="news",
                    timestamp_utc=ts,
                    text=article.title,
                    content=article.description,
                    metadata=article.model_dump(),
                )
                self.producer.send(self.topic, value=msg.model_dump(mode="json"))
                logger.info("NEWS | %s | %.50s", asset_name, article.title)
        except requests.RequestException as e:
            logger.error("NewsAPI error: %s", e)

    def run(self):
        if not self.queries:
            logger.warning("No news queries configured.")
            return
        logger.info("Starting NewsProducer ...")
        while True:
            try:
                start = time.time()
                for item in self.queries:
                    self._fetch(item["asset"], item["query"])
                    time.sleep(2)
                wait = max(0, self.POLL_INTERVAL - (time.time() - start))
                time.sleep(wait)
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error("News loop error: %s — retry in 60s", e)
                time.sleep(60)

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
