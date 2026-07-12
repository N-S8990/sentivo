"""Pydantic models for all data collector return types."""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, HttpUrl


class NewsSource(BaseModel):
    """Source metadata for a news article."""
    id: Optional[str] = None
    name: str


class NewsArticle(BaseModel):
    """A single news article from NewsAPI."""
    source: NewsSource
    author: Optional[str] = None
    title: str
    description: Optional[str] = None
    url: HttpUrl
    urlToImage: Optional[HttpUrl] = None
    publishedAt: str
    content: Optional[str] = None


class NewsApiResponse(BaseModel):
    """Top-level response from NewsAPI."""
    status: str
    totalResults: int
    articles: List[NewsArticle]


class RedditComment(BaseModel):
    """A single Reddit comment with score."""
    comment: str
    score: int


class RedditPost(BaseModel):
    """A Reddit post with metadata and top comments."""
    title: str
    score: int
    content: str
    comments: List[RedditComment]
    url: HttpUrl
    num_comments: int
    created_utc: float


class MarketDataPoint(BaseModel):
    """A single OHLCV market data point."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
