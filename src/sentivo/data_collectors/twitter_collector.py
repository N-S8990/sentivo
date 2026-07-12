"""Twitter/X data collector stub — API v2 requires Bearer token."""

import os

import tweepy
from dotenv import load_dotenv

load_dotenv()

auth = tweepy.OAuth1UserHandler(
    consumer_key=os.getenv("TWITTER_API_KEY"),
    consumer_secret=os.getenv("TWITTER_API_KEY_SECRET"),
    access_token=os.getenv("TWITTER_ACCESS_TOKEN"),
    access_token_secret=os.getenv("TWITTER_ACCESS_TOKEN_SECRET"),
)

api = tweepy.API(auth, wait_on_rate_limit=True)

if __name__ == "__main__":
    tweets = api.search_tweets(q="bitcoin", count=5)
    for t in tweets:
        print(t.text)
