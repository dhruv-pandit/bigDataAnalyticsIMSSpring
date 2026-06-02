import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

from market_data import SYMBOLS, fetch_trade, fetch_news


producer = KafkaProducer(
    bootstrap_servers=["localhost:8098"],
    value_serializer=lambda m: json.dumps(m).encode("utf-8"),
)


TRADE_POLL_SECONDS = 5
TRADES_PER_CYCLE   = 50          # random subset of SYMBOLS each cycle
NEWS_EVERY_N_CYCLES = 12         # once a minute
NEWS_PER_CYCLE      = 20         # random subset on each news cycle


def safe(symbol, fetcher, *args):
    """Run `fetcher(symbol, *args)`, log and swallow per-symbol errors."""
    try:
        return fetcher(symbol, *args)
    except Exception as exc:
        print(f"[{datetime.now()}] {symbol} {fetcher.__name__} failed: {exc}")
        return None


def sample(symbols, k):
    """Random sample of up to k symbols (clamped to len(symbols))."""
    return random.sample(symbols, k=min(k, len(symbols)))


if __name__ == "__main__":
    cycle = 0
    while True:
        for symbol in sample(SYMBOLS, TRADES_PER_CYCLE):
            trade = safe(symbol, fetch_trade)
            if trade:
                print(f"trade @ {datetime.now()} | {trade}")
                producer.send("trades", trade)

        if cycle % NEWS_EVERY_N_CYCLES == 0:
            for symbol in sample(SYMBOLS, NEWS_PER_CYCLE):
                # Each cycle re-emits every current RSS item with a fresh
                # observation timestamp - the join sees them as new events.
                items = safe(symbol, fetch_news) or []
                for item in items:
                    print(f"news  @ {datetime.now()} | {symbol}: {item['headline'][:80]}")
                    producer.send("news", item)

        producer.flush()
        cycle += 1
        time.sleep(TRADE_POLL_SECONDS)
