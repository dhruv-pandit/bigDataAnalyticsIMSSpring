"""Helpers for fetching market data from Yahoo Finance."""

import csv
import time
from pathlib import Path

import feedparser
import pendulum
import yfinance as yf


CONSTITUENTS_CSV = Path(__file__).parent / "sp500_constituents.csv"

RSS_URL = "https://finance.yahoo.com/rss/headline?s={symbol}"

# How many of the most-recent headlines to emit per fetch. Yahoo's RSS
# feed typically holds ~15-20 entries; we trim to a small most-recent
# subset to keep the news stream's volume sensible.
NEWS_ITEMS_PER_FETCH = 5

def _load_symbols():
    with CONSTITUENTS_CSV.open() as f:
        return [row["symbol"] for row in csv.DictReader(f)]

SYMBOLS = _load_symbols()

def fetch_trade(symbol: str):
    """Return a trade-tick payload for `symbol`, or None if Yahoo did
    not return a price (delisted, ticker not on Yahoo, off-hours).
    """
    info = yf.Ticker(symbol).fast_info
    price = info.get("lastPrice")
    # Treat both missing (None) and zero as "no price". A live equity
    # quote is never legitimately 0.0; Yahoo emits zeros for symbols
    # it has no recent quote for.
    if not price:
        return None
    return {
        "symbol": symbol,
        "price": float(price),
        "volume": int(info.get("lastVolume") or 0),
        "timestamp": pendulum.now().to_datetime_string(),
    }


def fetch_news(symbol: str):
    """Return the `NEWS_ITEMS_PER_FETCH` most recent RSS items for
    `symbol` with a fresh observation `timestamp`. We do not perform 
    deduplication; thus each call is a new observation event, even if Yahoo's article
    list is unchanged.

    Since we don't have direct access to a real-time news feed,
    we do this for pedagogical purposes.
    """
    feed = feedparser.parse(RSS_URL.format(symbol=symbol))
    # Yahoo returns entries in reverse-chrono order
    entries = sorted(
        (e for e in feed.entries if e.get("link") and e.get("published_parsed")),
        key=lambda e: e.published_parsed,
        reverse=True,
    )[:NEWS_ITEMS_PER_FETCH]

    now = pendulum.now().to_datetime_string()
    items = []
    for entry in entries:
        published = pendulum.from_timestamp(
            time.mktime(entry.published_parsed)
        ).to_datetime_string()
        items.append({
            "symbol": symbol,
            "headline": entry.get("title", ""),
            "link": entry["link"],
            "published": published,
            "timestamp": now,
        })
    return items