"""Main entry point — CLI dispatcher for all producers and consumers."""

import argparse
import logging
import sys

from sentivo.consumers import (
    AggregatorConsumer,
    LoggingSink,
    SentimentConsumer,
    SignalConsumer,
)
from sentivo.core.config import load_config
from sentivo.core.logging_config import setup_logging
from sentivo.producers.market_data_producer import MarketDataProducer
from sentivo.producers.news_producer import NewsProducer
from sentivo.producers.reddit_producer import RedditProducer

logger = logging.getLogger(__name__)


def main():
    setup_logging()

    try:
        config = load_config()
    except FileNotFoundError as e:
        logger.error("FATAL: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.error("FATAL: config error: %s", e)
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Sentivo — sentiment & market pulse engine")
    sub = parser.add_subparsers(dest="service_type", required=True)

    # --- producers ---
    prod = sub.add_parser("producer", help="Run a data producer")
    psub = prod.add_subparsers(dest="producer_name", required=True)
    psub.add_parser("reddit", help="Reddit streaming producer")
    psub.add_parser("news", help="NewsAPI polling producer")
    psub.add_parser("market", help="Market data polling producer")

    # --- consumers ---
    con = sub.add_parser("consumer", help="Run a data consumer")
    csub = con.add_subparsers(dest="consumer_name", required=True)
    csub.add_parser("sentiment", help="Sentiment analysis consumer")
    csub.add_parser("aggregator", help="Fear & Greed aggregator consumer")
    csub.add_parser("signal", help="Trade signal generator")
    csub.add_parser("logger", help="Signal logging sink")

    args = parser.parse_args()
    service = None

    try:
        if args.service_type == "producer":
            assets = config.get("assets", [])
            if not assets:
                logger.warning("No assets defined — exiting.")
                sys.exit(0)

            if args.producer_name == "reddit":
                service = RedditProducer(config)
            elif args.producer_name == "news":
                service = NewsProducer(config)
            elif args.producer_name == "market":
                service = MarketDataProducer(config)

        elif args.service_type == "consumer":
            if args.consumer_name == "sentiment":
                service = SentimentConsumer(config)
            elif args.consumer_name == "aggregator":
                service = AggregatorConsumer(config)
            elif args.consumer_name == "signal":
                service = SignalConsumer()
            elif args.consumer_name == "logger":
                service = LoggingSink()

        if service:
            logger.info("Starting %s ...", type(service).__name__)
            service.run()
        else:
            parser.print_help()

    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.critical("Unhandled error: %s", e, exc_info=True)
    finally:
        if service:
            service.close()


if __name__ == "__main__":
    main()
