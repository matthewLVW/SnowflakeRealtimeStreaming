#!/usr/bin/env python3
"""Kafka producer that streams ticker prices from Yahoo Finance."""

import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yfinance as yf  # type: ignore
from kafka import KafkaProducer  # type: ignore
from kafka.errors import KafkaError, NoBrokersAvailable  # type: ignore

HISTORY_INITIAL_WAIT = max(15.0, float(os.environ.get("HISTORY_BASE_SEC", "30")))
HISTORY_MAX_WAIT = max(float(os.environ.get("HISTORY_MAX_SEC", "300")), HISTORY_INITIAL_WAIT)


def load_symbols() -> List[str]:
    raw = os.environ.get("SYMS", "AAPL").strip()
    return [sym.strip().upper() for sym in raw.split(",") if sym.strip()]


def clamp_poll_seconds(val: int) -> int:
    if val < 10:
        return 10
    if val > 30:
        return 30
    return val


def make_event(sym: str) -> Tuple[Dict[str, Any], yf.Ticker]:
    ticker = yf.Ticker(sym)
    info = getattr(ticker, "fast_info", {}) or {}

    now = time.time()
    size = (
        info.get("last_size")
        or info.get("bid_size")
        or info.get("ask_size")
        or None
    )

    event: Dict[str, Any] = {
        "symbol": sym,
        "px": info.get("last_price"),
        "bid": info.get("bid"),
        "ask": info.get("ask"),
        "sz": size,
        "event_ts": now,
        "source": "yfinance.fast_info",
        "ingest_ts": now,
    }
    return event, ticker


def init_producer(brokers: str) -> Optional[KafkaProducer]:
    try:
        return KafkaProducer(
            bootstrap_servers=brokers.split(","),
            value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        )
    except NoBrokersAvailable:
        print("[producer] Kafka not available; falling back to local JSONL logging.", file=sys.stderr)
        return None


def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, separators=(",", ":")) + "\n")


def merge_cached_event(event: Dict[str, Any], cached: Dict[str, Any]) -> None:
    for key in ("px", "bid", "ask", "sz", "event_ts", "source"):
        if event.get(key) is None and cached.get(key) is not None:
            event[key] = cached[key]


def history_fallback(
    sym: str,
    ticker: yf.Ticker,
    event: Dict[str, Any],
    throttle: Dict[str, Dict[str, float]],
    cache: Dict[str, Dict[str, Any]],
) -> None:
    now = time.time()
    state = throttle.setdefault(sym, {"wait": HISTORY_INITIAL_WAIT, "next": 0.0})

    if now < state["next"]:
        cached = cache.get(sym)
        if cached:
            merge_cached_event(event, cached)
        return

    print(f"[producer] history fallback fetch for {sym}", file=sys.stderr)

    try:
        history = ticker.history(period="1d", interval="1m")
    except Exception as exc:  # pragma: no cover - network fails
        print(f"[producer] history fallback failed for {sym}: {exc}", file=sys.stderr)
        state["wait"] = min(state["wait"] * 2, HISTORY_MAX_WAIT)
        state["next"] = now + state["wait"]
        cached = cache.get(sym)
        if cached:
            merge_cached_event(event, cached)
        return

    if history.empty:
        state["wait"] = min(state["wait"] * 2, HISTORY_MAX_WAIT)
        state["next"] = now + state["wait"]
        cached = cache.get(sym)
        if cached:
            merge_cached_event(event, cached)
        return

    last_row = history.tail(1)
    row = last_row.iloc[0]
    price = row.get("Close")

    if price is None or price != price:
        state["wait"] = min(state["wait"] * 2, HISTORY_MAX_WAIT)
        state["next"] = now + state["wait"]
        cached = cache.get(sym)
        if cached:
            merge_cached_event(event, cached)
        return

    event["px"] = float(price)
    if event.get("bid") is None:
        event["bid"] = event["px"]
    if event.get("ask") is None:
        event["ask"] = event["px"]

    volume = row.get("Volume")
    if volume is not None and volume == volume and event.get("sz") is None:
        event["sz"] = float(volume)

    index = last_row.index[-1]
    if hasattr(index, "timestamp"):
        event["event_ts"] = float(index.timestamp())
    event["source"] = "yfinance.history"

    cache[sym] = {
        "px": event["px"],
        "bid": event.get("bid"),
        "ask": event.get("ask"),
        "sz": event.get("sz"),
        "event_ts": event.get("event_ts"),
        "source": event["source"],
    }

    state["wait"] = HISTORY_INITIAL_WAIT
    state["next"] = now + state["wait"]


def main() -> None:
    symbols = load_symbols()
    poll_env = int(os.environ.get("POLL_SEC", "15"))
    poll_sec = clamp_poll_seconds(poll_env)

    brokers = os.environ.get("KAFKA_BROKERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "ticks.raw")

    default_log = Path(tempfile.gettempdir()) / "ticks.log"
    log_path = Path(os.environ.get("TICKS_LOG", str(default_log)))

    producer = init_producer(brokers)

    history_throttle: Dict[str, Dict[str, float]] = {}
    history_cache: Dict[str, Dict[str, Any]] = {}

    while True:
        for sym in symbols:
            event, ticker = make_event(sym)
            if event.get("px") is None:
                history_fallback(sym, ticker, event, history_throttle, history_cache)
            print(json.dumps(event, separators=(",", ":")))

            if producer is not None:
                try:
                    producer.send(topic, event)
                except KafkaError as exc:
                    print(f"[producer] Kafka send failed: {exc}; writing to {log_path}", file=sys.stderr)
                    append_jsonl(log_path, event)
            else:
                append_jsonl(log_path, event)

        if producer is not None:
            try:
                producer.flush(timeout=5)
            except KafkaError as exc:
                print(f"[producer] Kafka flush failed: {exc}; continuing with fallback logging only.", file=sys.stderr)
                producer = None

        time.sleep(poll_sec)


if __name__ == "__main__":
    main()
