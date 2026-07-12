"""Sentiment analysis consumer.
Reads raw_text_data, runs FinBERT inference, publishes analyzed_sentiment.
"""

import logging
from typing import Any, Dict, Set

from kafka import KafkaConsumer, KafkaProducer

from sentivo.consumers.base import BaseConsumer  # import from consumers/base
from sentivo.core.kafka_client import get_kafka_consumer, get_kafka_producer
from sentivo.schemas import AnalyzedSentimentMessage, RawTextMessage
from sentivo.sentiment_analysis.onnx_finbert import OnnxFinBert
from sentivo.sentiment_analysis.text_preprocessor import TextPreprocessor

logger = logging.getLogger(__name__)


class SentimentConsumer(BaseConsumer):
    """Consumes raw text, classifies sentiment, and emits analysed results."""

    def __init__(self, config: Dict[str, Any]):
        logger.info("Initialising SentimentConsumer ...")
        self.consumer: KafkaConsumer = get_kafka_consumer(
            topic="raw_text_data", group_id="sentiment-analyzers"
        )
        self.producer: KafkaProducer = get_kafka_producer()
        self.in_topic = "raw_text_data"
        self.out_topic = "analyzed_sentiment"

        logger.info("Loading sentiment model (FinBERT ONNX) ...")
        self.model = OnnxFinBert()
        self.preprocessor = TextPreprocessor()

        self.assets = config.get("assets", [])
        self.keyword_map: Dict[str, str] = {}
        for asset in self.assets:
            for kw in asset.get("keywords", []):
                self.keyword_map[kw.lower()] = asset["name"]
        logger.info("NER map built (%d keywords).", len(self.keyword_map))

    def _resolve_assets(self, text: str) -> Set[str]:
        """Keyword-based named entity resolution."""
        found = set()
        for word in text.lower().split():
            cleaned = word.strip(".,!?:;()\"'")
            if cleaned in self.keyword_map:
                found.add(self.keyword_map[cleaned])
        return found

    def run(self):
        try:
            for msg in self.consumer:
                try:
                    data = RawTextMessage.model_validate(msg.value)

                    prep_text = self.preprocessor.preprocess(data.text)
                    prep_content = self.preprocessor.preprocess(data.content)
                    to_analyze = [t for t in [prep_text, prep_content] if t]

                    if not to_analyze:
                        continue

                    sentiments = self.model.predict(to_analyze)
                    if not sentiments:
                        continue

                    avg = [sum(c) / len(c) for c in zip(*sentiments)]
                    score = float(avg[0] - avg[1])  # pos - neg

                    targets = set()
                    if data.asset_name == "general":
                        found = self._resolve_assets(prep_text)
                        found.update(self._resolve_assets(prep_content))
                        if not found:
                            continue
                        targets = found
                    else:
                        targets.add(data.asset_name)

                    for asset in targets:
                        out = AnalyzedSentimentMessage(
                            asset_name=asset,
                            source=data.source,
                            timestamp_utc=data.timestamp_utc,
                            sentiment_score=score,
                            sentiment_probs=avg,
                            metadata=data.metadata,
                        )
                        self.producer.send(
                            self.out_topic, value=out.model_dump()
                        )
                        logger.info(
                            "SENTIMENT | %-10s | %.3f | %.40s",
                            asset, score, data.text,
                        )
                except Exception as e:
                    logger.error("Processing error: %s", e)
        except KeyboardInterrupt:
            pass
        finally:
            self.close()

    def close(self):
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.close()
