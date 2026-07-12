"""Reddit streaming producer — pushes new submissions to Kafka."""

import logging
import os
import time
from typing import Any, Dict

import praw
from kafka import KafkaProducer

from sentivo.core.kafka_client import get_kafka_producer
from sentivo.producers.base import BaseProducer
from sentivo.schemas import RawTextMessage, RedditPost

logger = logging.getLogger(__name__)


class RedditProducer(BaseProducer):
    """Streams Reddit submissions from tracked subreddits to 'raw_text_data'."""

    def __init__(self, config: Dict[str, Any]):
        logger.info("Initialising RedditProducer ...")
        self.producer: KafkaProducer = get_kafka_producer()
        self.assets = config.get("assets", [])
        self.general_sources = config.get("general_reddit_sources", [])
        self.reddit: praw.Reddit = self._build_client()
        self.topic = "raw_text_data"
        self.sub_map: Dict[str, str] = {}
        all_subs = set()

        for asset in self.assets:
            name = asset["name"]
            for sub in asset.get("reddit_subreddits", []):
                s = sub.lower()
                all_subs.add(s)
                self.sub_map[s] = name

        for sub in self.general_sources:
            s = sub.lower()
            all_subs.add(s)
            self.sub_map[s] = "general"

        self.sub_list = "+".join(all_subs)
        self.subreddit = None
        if self.sub_list:
            logger.info("Monitoring subreddits: %s", self.sub_list)
            self.subreddit = self.reddit.subreddit(self.sub_list)
        else:
            logger.warning("No subreddits configured.")

    def _build_client(self) -> praw.Reddit:
        return praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=os.getenv("REDDIT_USER_AGENT"),
            read_only=True,
        )

    def run(self):
        if not self.subreddit:
            return
        logger.info("Starting Reddit stream ...")
        while True:
            try:
                for post in self.subreddit.stream.submissions(skip_existing=True):
                    asset = self.sub_map.get(post.subreddit.display_name.lower())
                    if not asset:
                        continue

                    reddit_post = RedditPost(
                        title=post.title,
                        score=post.score,
                        content=post.selftext,
                        comments=[],
                        url=post.url,
                        num_comments=post.num_comments,
                        created_utc=post.created_utc,
                    )

                    msg = RawTextMessage(
                        asset_name=asset,
                        source="reddit",
                        timestamp_utc=post.created_utc,
                        text=post.title,
                        content=post.selftext,
                        metadata=reddit_post.model_dump(),
                    )
                    self.producer.send(self.topic, value=msg.model_dump())
                    logger.info("REDDIT | %s | %.50s", asset, post.title)
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error("Stream error: %s — retry in 60s", e)
                time.sleep(60)

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
