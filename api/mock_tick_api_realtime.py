#!/usr/bin/env python3
"""Serve historical ticks as if they were live by shifting timestamps to "now"."""

from __future__ import annotations

import csv
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, Optional

from flask import Flask, Response, jsonify, stream_with_context

TickEvent = Dict[str, object]

app = Flask(__name__)


def env_bool(key: str, default: str = "false") -> bool:
    return os.environ.get(key, default).strip().lower() in {"1", "true", "yes", "y"}


def parse_float(value: str) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def market_open_filter(dt: datetime) -> bool:
    market_open = os.environ.get("MOCK_MARKET_OPEN_LOCAL", "09:30")
    try:
        hours, minutes = [int(part) for part in market_open.split(":", 1)]
    except ValueError:
        hours, minutes = 9, 30
    open_dt = dt.replace(hour=hours, minute=minutes, second=0, microsecond=0)
    return dt >= open_dt


def iter_events(config: Dict[str, object]) -> Iterator[TickEvent]:
    data_path: Path = config["data_path"]  # type: ignore[assignment]
    symbol: str = config["symbol"]  # type: ignore[assignment]
    speed: float = config["speed"]  # type: ignore[assignment]
    max_sleep: float = config["max_sleep"]  # type: ignore[assignment]
    loop: bool = config["loop"]  # type: ignore[assignment]
    include_source_ts: bool = config["include_source_ts"]  # type: ignore[assignment]

    live_offset_sec: Optional[float] = None

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

                event_ts_original = dt.timestamp()

                if live_offset_sec is None:
                    live_offset_sec = time.time() - event_ts_original
                event_ts_live = event_ts_original + live_offset_sec

                if last_ts is not None:
                    delta = max(event_ts_original - last_ts, 0.0)
                    sleep_for = delta / speed if speed > 0 else 0.0
                    if max_sleep >= 0:
                        sleep_for = min(sleep_for, max_sleep)
                    if sleep_for > 0:
                        time.sleep(sleep_for)
                last_ts = event_ts_original

                payload: Dict[str, object] = {
                    "symbol": symbol,
                    "px": parse_float(price),
                    "bid": parse_float(bid),
                    "ask": parse_float(ask),
                    "sz": parse_float(size),
                    "event_ts": event_ts_live,
                    "api_send_ts_ms": int(time.time() * 1000),
                    "source": "mock_api_realtime",
                }
                if include_source_ts:
                    payload["source_event_ts"] = event_ts_original
                    payload["source_event_ts_ms"] = int(round(event_ts_original * 1000))

                yield payload
        if not loop:
            break


def build_config() -> Dict[str, object]:
    data_path = Path(os.environ.get("MOCK_TICK_FILE", "data/samples/WDC_tickbidask_week1.txt"))
    if not data_path.exists():
        raise FileNotFoundError(f"Tick file not found: {data_path}")

    speed_env = os.environ.get("MOCK_STREAM_SPEED")
    if speed_env is None:
        speed_env = os.environ.get("REPLAY_SPEED")
    speed = float(speed_env or "1.0")

    max_sleep = float(os.environ.get("MOCK_MAX_SLEEP", "1.0"))

    return {
        "data_path": data_path,
        "symbol": os.environ.get("MOCK_SYMBOL", "WDC"),
        "speed": speed,
        "max_sleep": max_sleep,
        "loop": env_bool("MOCK_LOOP", "false"),
        "include_source_ts": env_bool("MOCK_INCLUDE_SOURCE_TS", "true"),
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
