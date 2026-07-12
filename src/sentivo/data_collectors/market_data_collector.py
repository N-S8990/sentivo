"""Market data collector using yfinance."""

from typing import List

import yfinance as yf

from sentivo.data_collectors.base import BaseCollector
from sentivo.data_collectors.models import MarketDataPoint


class MarketDataCollector(BaseCollector):
    """Downloads OHLCV history for a given ticker."""

    def fetch_data(
        self, ticker: str, period: str = "1mo", interval: str = "1d"
    ) -> List[MarketDataPoint]:
        """Fetch historical market data."""
        stock = yf.Ticker(ticker)
        hist_df = stock.history(period=period, interval=interval)
        if hist_df.empty:
            print(f"No data for {ticker} ({period}/{interval})")
            return []

        hist_df.reset_index(inplace=True)

        ts_col = next(
            (c for c in hist_df.columns if "Date" in c or "Time" in c), None
        )
        if not ts_col:
            raise ValueError("Could not locate timestamp column in yfinance data.")

        hist_df.rename(columns={
            ts_col: "timestamp",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }, inplace=True)

        cols = ["timestamp", "open", "high", "low", "close", "volume"]
        records = hist_df[cols].to_dict(orient="records")
        return [MarketDataPoint.model_validate(r) for r in records]
