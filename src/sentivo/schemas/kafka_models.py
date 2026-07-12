"""Kafka message schemas — every topic has a corresponding Pydantic model."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class RawTextMessage(BaseModel):
    """Schema for messages on the 'raw_text_data' topic."""
    asset_name: str
    source: str
    timestamp_utc: float
    text: str
    content: Optional[str] = None
    metadata: Dict[str, Any]


class RawMarketMessage(BaseModel):
    """Schema for messages on the 'raw_market_data' topic."""
    ticker: str
    timestamp_utc: float
    price: float
    volume: float


class AnalyzedSentimentMessage(BaseModel):
    """Schema for messages on the 'analyzed_sentiment' topic."""
    asset_name: str
    source: str
    timestamp_utc: float
    sentiment_score: float
    sentiment_probs: List[float]
    metadata: Dict[str, Any]


class AggregatedMetricsMessage(BaseModel):
    """Schema for the 'aggregated_metrics' topic — the Fear & Greed index."""
    asset_name: str
    ticker: str
    timestamp_utc: float
    sentiment_1min_avg: Optional[float] = None
    sentiment_5min_avg: Optional[float] = None
    sentiment_15min_avg: Optional[float] = None
    sentiment_velocity: Optional[float] = None
    price: float
    volume: float
    price_change_1min_pct: Optional[float] = None
    price_change_5min_pct: Optional[float] = None
    fear_greed_score: float


class TradeSignalMessage(BaseModel):
    """Schema for the 'trade_signals' topic."""
    asset_name: str
    ticker: str
    timestamp_utc: float
    signal: str
    confidence: float
    reason: str
    fear_greed_score: float
