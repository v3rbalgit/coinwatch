# src/adapters/sentiment/__init__.py

from .reddit import RedditAdapter
from .twitter import TwitterAdapter
from .telegram import TelegramAdapter

__all__ = [
    'RedditAdapter',
    'TwitterAdapter',
    'TelegramAdapter'
]