"""News data collector via NewsAPI."""

import os

import requests
from dotenv import load_dotenv

from sentivo.data_collectors.base import BaseCollector
from sentivo.data_collectors.models import NewsApiResponse
from sentivo.sentiment_analysis.text_preprocessor import TextPreprocessor

load_dotenv()


class NewsCollector(BaseCollector):
    """Fetches financial news articles from NewsAPI."""

    def __init__(self):
        self.api_key = os.getenv("NEWSAPI_API_KEY")
        if not self.api_key:
            raise ValueError("NEWSAPI_API_KEY environment variable not set")
        self.base_url = "https://newsapi.org/v2/everything"
        self.preprocessor = TextPreprocessor()

    def fetch_data(self, query="all", page_size=10) -> NewsApiResponse:
        """Query news articles by keyword."""
        params = {
            "q": query,
            "apiKey": self.api_key,
            "pageSize": page_size,
        }

        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            return NewsApiResponse.model_validate(response.json())
        except requests.exceptions.RequestException as e:
            print(f"News fetch error: {e}")
            return NewsApiResponse(status="error", totalResults=0, articles=[])

    def preprocess_data(self, news_data: NewsApiResponse) -> NewsApiResponse:
        """Normalise article titles and descriptions."""
        for article in news_data.articles:
            article.title = self.preprocessor.preprocess(article.title)
            if article.description:
                article.description = self.preprocessor.preprocess(article.description)
        return news_data
