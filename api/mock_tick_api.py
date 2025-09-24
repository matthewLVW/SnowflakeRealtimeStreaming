#!/usr/bin/env python3
"""Serve historical ticks as a mock streaming API for the pipeline."""

from __future__ import annotations

import csv
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterator, Optional

from flask import Flask, Response, jsonify, stream_with_context

TickEvent = Dict[str, object]

app = Flask(__name__)

MARKET_OPEN_UTC = (14, 30, 0)  # default: 14:30:00 UTC (9:30 ET)


def env_bool(key: str, default: str = "false") -> bool:
    return os.environ.get(key, default).strip().lower() in {"1", "true", "yes", "y"}


def parse_float(value: str) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def market_open_filter(dt: datetime) -> bool:
    override = os.environ.get("MOCK_MARKET_OPEN_UTC")
    hours, minutes, seconds = MARKET_OPEN_UTC
    if override:
        try:
            parts = [int(part) for part in override.split(":", 2)]
            while len(parts) < 3:
                parts.append(0)
            hours, minutes, seconds = parts[:3]
        except ValueError:
            pass
    open_time = dt.replace(hour=hours, minute=minutes, second=seconds, microsecond=0)
    if dt.hour < hours:
        open_time = open_time
    return dt >= open_time


def iter_events(config: Dict[str, object]) -> Iterator[TickEvent]:
    data_path: Path = config["data_path"]  # type: ignore[assignment]
    symbol: str = config["symbol"]  # type: ignore[assignment]
    speed: float = config["speed"]  # type: ignore[assignment]
    max_sleep: float = config["max_sleep"]  # type: ignore[assignment]
    loop: bool = config["loop"]  # type: ignore[assignment]

    while True:
        last_ts: Optional[float] = None
        with data_path.open("r", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            for row in reader:
                if not row or row[0].startswith("#"):
                    continue
                try:
                    date_str, time_str, price, bid, ask, size = row[:6]
                except ValueError:
                    continue

                dt = datetime.strptime(f"{date_str} {time_str}", "%m/%d/%Y %H:%M:%S")
                if not market_open_filter(dt):
                    continue
                event_ts = dt.timestamp()

                if last_ts is not None:
                    delta = max(event_ts - last_ts, 0.0)
                    sleep_for = delta / speed if speed > 0 else 0.0
                    if max_sleep >= 0:
                        sleep_for = min(sleep_for, max_sleep)
                    if sleep_for > 0:
                        time.sleep(sleep_for)
                last_ts = event_ts

                yield {
                    "symbol": symbol,
                    "px": parse_float(price),
                    "bid": parse_float(bid),
                    "ask": parse_float(ask),
                    "sz": parse_float(size),
                    "event_ts": event_ts,
                    "ingest_ts": time.time(),
                    "source": "mock_api",
                }
        if not loop:
            break


def build_config() -> Dict[str, object]:
    data_path = Path(os.environ.get("MOCK_TICK_FILE", "data/samples/WDC_tickbidask_week1.txt"))
    if not data_path.exists():
        raise FileNotFoundError(f"Tick file not found: {data_path}")

    speed = float(os.environ.get("MOCK_STREAM_SPEED", "1.0"))
    if speed <= 0:
        raise ValueError("MOCK_STREAM_SPEED must be positive")

    max_sleep = float(os.environ.get("MOCK_MAX_SLEEP", "1.0"))

    return {
        "data_path": data_path,
        "symbol": os.environ.get("MOCK_SYMBOL", "WDC"),
        "speed": speed,
        "max_sleep": max_sleep,
        "loop": env_bool("MOCK_LOOP", "false"),
    }


@app.route("/health")
def health() -> Response:
    try:
        config = build_config()
    except (FileNotFoundError, ValueError) as exc:
        return jsonify({"status": "error", "detail": str(exc)}), 500
    return jsonify({"status": "ok", "file": str(config["data_path"])})


@app.route("/ticks")
def stream_ticks() -> Response:
    try:
        config = build_config()
    except (FileNotFoundError, ValueError) as exc:
        return jsonify({"error": str(exc)}), 500

    def generate() -> Iterator[str]:
        for event in iter_events(config):
            yield json.dumps(event, separators=(",", ":")) + "\n"

    return Response(stream_with_context(generate()), mimetype="application/x-ndjson")


def main() -> None:
    host = os.environ.get("MOCK_API_HOST", "0.0.0.0")
    port = int(os.environ.get("MOCK_API_PORT", "8000"))
    debug = env_bool("MOCK_API_DEBUG", "false")
    app.run(host=host, port=port, debug=debug, threaded=True)


if __name__ == "__main__":
    main()
