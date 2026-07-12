# Sentivo — Market Sentiment & Pulse Engine

Real-time sentiment analysis and signal generation engine that aggregates social media, news, and market data into actionable Fear & Greed scores and trade signals.

## Architecture

```
Reddit ──────> RedditProducer ──┐
                                 ├── raw_text_data ──> SentimentConsumer ──> analyzed_sentiment ──┐
NewsAPI ─────> NewsProducer   ──┘                                                                 │
                                                                                                   ├── AggregatorConsumer ──> aggregated_metrics ──> SignalConsumer ──> trade_signals ──> LoggingSink
yFinance ────> MarketDataProducer ──> raw_market_data ──────────────────────────────────────────────┘
```

### Kafka Topics

| Topic | Payload | Produced By | Consumed By |
|-------|---------|-------------|-------------|
| `raw_text_data` | Raw text from Reddit / News | RedditProducer, NewsProducer | SentimentConsumer |
| `raw_market_data` | Price + volume snapshots | MarketDataProducer | AggregatorConsumer |
| `analyzed_sentiment` | FinBERT sentiment score [-1, 1] | SentimentConsumer | AggregatorConsumer |
| `aggregated_metrics` | Fear & Greed Index (0–100) | AggregatorConsumer | SignalConsumer |
| `trade_signals` | BUY / SELL / HOLD with confidence | SignalConsumer | LoggingSink |

## Quick Start

### Prerequisites

- Python 3.11–3.14
- Docker & Docker Compose
- Poetry (recommended) or pip

### Setup

```bash
# Clone & enter the project
cd sentivo
poetry shell

# Install dependencies
poetry install

# Configure API keys
cp .env.example .env
# Edit .env with your keys

# Start Kafka
docker compose up -d

# Run the full pipeline (tmux)
./scripts/run_all.sh
```

### Run Individual Components

```bash
# Docker services (Kafka + Zookeeper)
docker compose up

# Terminal 2 — producers
poetry run python src/sentivo/main.py producer reddit
poetry run python src/sentivo/main.py producer news
poetry run python src/sentivo/main.py producer market

# Terminal 3 — consumers
poetry run python src/sentivo/main.py consumer sentiment
poetry run python src/sentivo/main.py consumer aggregator
poetry run python src/sentivo/main.py consumer signal
poetry run python src/sentivo/main.py consumer logger
```

### Required API Keys

Add these to `.env`:

- `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, `REDDIT_USER_AGENT` — [Reddit App](https://www.reddit.com/prefs/apps)
- `NEWSAPI_API_KEY` — [NewsAPI](https://newsapi.org/)
- `TWITTER_*` — Optional, for Twitter/X data

## Configuration

Edit `config/targets.yaml` to define tracked assets:

```yaml
assets:
  - name: "Bitcoin"
    ticker: "BTC-USD"
    keywords: ["bitcoin", "btc"]
    reddit_subreddits: ["bitcoin", "btc", "CryptoCurrency"]
    news_queries: ["bitcoin", "crypto"]
```

## Sentiment Analysis

Uses **FinBERT** (ProsusAI/finbert) exported to ONNX with dynamic quantisation for CPU inference. Each batch of 32 texts processes in ~50ms.

**Preprocessing pipeline:**
1. Lowercase normalisation
2. URL / mention / ticker removal
3. Whitespace cleanup
4. LRU cache (10 000 entries)

## Fear & Greed Index

Three weighted components:

| Component | Weight | Range |
|-----------|--------|-------|
| Sentiment score (5 min avg) | 50% | 0–100 |
| Price momentum (5 min Δ) | 30% | 0–100 |
| Sentiment velocity | 20% | 0–100 |

## Signal Strategies

1. **Contrarian Buy** — F&G < 20 + sentiment recovering → BUY (75% confidence)
2. **Contrarian Sell** — F&G > 85 + price dipping → SELL (80% confidence)
3. **Trend Follow** — F&G > 70 + strong sentiment + rising price → BUY (60% confidence)

## Project Structure

```
src/sentivo/
├── main.py                    # CLI entry point
├── core/
│   ├── config.py              # Env + YAML loader
│   ├── logging_config.py      # stdout logging setup
│   └── kafka_client.py        # Kafka producer/consumer factory
├── data_collectors/           # API wrappers (Reddit, News, Market, Twitter)
├── producers/                 # Kafka producers (streaming & polling)
├── consumers/                 # Kafka consumers (sentiment → aggregation → signal)
├── schemas/                   # Pydantic message models
└── sentiment_analysis/        # FinBERT ONNX, preprocessor, test texts
```


## Performance

- **Throughput**: 1 000+ messages/min across all Kafka topics
- **Sentiment latency**: <100 ms per batch of 32 texts (ONNX CPU)
- **Signal latency**: <500 ms from data ingestion to final signal
- **Memory**: ~2 GB with loaded FinBERT model
- **CPU**: ~40% on 4-core system at peak load

## Methodology

The Fear & Greed Index combines behavioural finance principles with real-time NLP:

1. **Sentiment score** (50 %) — 5-minute rolling average of FinBERT scores normalised to 0–100
2. **Price momentum** (30 %) — 5-minute percent change capped at ±5 %
3. **Sentiment velocity** (20 %) — rate of change in 1-minute sentiment averages

Signals are generated when threshold combinations indicate statistically significant market conditions, inspired by classic behavioural finance literature (Shiller, Kahneman & Tversky).
