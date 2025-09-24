#!/usr/bin/env python3
"""Replay ticks from the mock API, emit 10 ms bins into Kafka.""" 

from __future__ import annotations

import json
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, Optional

import requests  # type: ignore
from kafka import KafkaProducer  # type: ignore
from kafka.errors import KafkaError, NoBrokersAvailable  # type: ignore


@dataclass
class ProducerConfig:
    brokers: str
    topic: str
    api_url: str
    bin_ms: int
    max_late_ms: int
    symbol: str
    replay_speed: float
    log_path: Path
    dlt_path: Path


@dataclass
class BinState:
    symbol: str
    start_ms: int
    window_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    notional: float
    trade_count: int
    max_lateness_ms: int = 0

    def to_payload(self, ingest_ts_ms: int) -> Dict[str, object]:
        bin_id = f"{self.symbol}-{self.start_ms:013d}"
        return {
            "bin_id": bin_id,
            "symbol": self.symbol,
            "start_ts_ms": self.start_ms,
            "end_ts_ms": self.start_ms + self.window_ms,
            "window_ms": self.window_ms,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": round(self.volume, 6),
            "notional": round(self.notional, 6),
            "trade_count": self.trade_count,
            "max_lateness_ms": self.max_lateness_ms,
            "ingest_ts_ms": ingest_ts_ms,
            "source": "replay",
        }


def init_producer(brokers: str) -> Optional[KafkaProducer]:
    try:
        return KafkaProducer(
            bootstrap_servers=brokers.split(","),
            key_serializer=lambda v: v.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        )
    except NoBrokersAvailable:
        print("[replay] Kafka unavailable; falling back to file logging only.", file=sys.stderr)
        return None


def append_jsonl(path: Path, obj: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(obj, separators=(",", ":")) + "\n")


def load_config() -> ProducerConfig:
    brokers = os.environ.get("KAFKA_BROKERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", os.environ.get("KAFKA_TOPIC_RAW", "ticks.raw"))
    api_url = os.environ.get("MOCK_API_URL", "http://localhost:8000/ticks")
    symbol = os.environ.get("REPLAY_SYMBOL", "WDC")
    bin_ms = int(os.environ.get("REPLAY_BIN_MS", "10"))
    if bin_ms <= 0:
        raise ValueError("REPLAY_BIN_MS must be positive")
    max_late_ms = int(os.environ.get("REPLAY_MAX_LATE_MS", "500"))
    replay_speed = float(os.environ.get("REPLAY_SPEED", "0"))
    log_path = Path(os.environ.get("REPLAY_LOG", "logs/replay.jsonl"))
    dlt_path = Path(os.environ.get("REPLAY_DLT", "logs/replay_dlt.jsonl"))
    return ProducerConfig(
        brokers=brokers,
        topic=topic,
        api_url=api_url,
        bin_ms=bin_ms,
        max_late_ms=max_late_ms,
        symbol=symbol,
        replay_speed=replay_speed,
        log_path=log_path,
        dlt_path=dlt_path,
    )


def stream_ticks(url: str) -> Iterator[Dict[str, object]]:
    while True:
        try:
            with requests.get(url, stream=True, timeout=1000) as response:
                response.raise_for_status()
                for raw_line in response.iter_lines():
                    if not raw_line:
                        continue
                    try:
                        yield json.loads(raw_line)
                    except json.JSONDecodeError as exc:
                        print(f"[replay] Failed to decode line: {exc}", file=sys.stderr)
        except requests.RequestException as exc:  # pragma: no cover - network errors
            print(f"[replay] Reconnecting after error: {exc}", file=sys.stderr)
            time.sleep(1.0)


def epoch_ms(value: float) -> int:
    return int(round(value * 1000.0))


def floor_bin(ts_ms: int, window_ms: int) -> int:
    return (ts_ms // window_ms) * window_ms


def process_stream(config: ProducerConfig, producer: Optional[KafkaProducer]) -> None:
    symbol_bins: Dict[str, Dict[int, BinState]] = defaultdict(dict)
    watermarks: Dict[str, int] = defaultdict(int)
    last_emit_ts_ms: Dict[str, int] = defaultdict(int)

    for tick in stream_ticks(config.api_url):
        tick_symbol = str(tick.get("symbol") or config.symbol)
        price = tick.get("px")
        size = tick.get("sz")
        event_ts = tick.get("event_ts")
        if price is None or event_ts is None:
            continue
        price_f = float(price)
        event_ts_ms = epoch_ms(float(event_ts))
        size_f = float(size) if isinstance(size, (int, float)) else 0.0

        watermark = watermarks[tick_symbol]
        lateness = max(watermark - event_ts_ms, 0)
        if event_ts_ms > watermark:
            watermarks[tick_symbol] = event_ts_ms
            lateness = 0

        if lateness > config.max_late_ms:
            append_jsonl(
                config.dlt_path,
                {
                    "reason": "late_tick",
                    "lateness_ms": lateness,
                    "symbol": tick_symbol,
                    "event_ts_ms": event_ts_ms,
                    "tick": tick,
                },
            )
            continue

        bin_start = floor_bin(event_ts_ms, config.bin_ms)
        state = symbol_bins[tick_symbol].get(bin_start)
        if state is None:
            state = BinState(
                symbol=tick_symbol,
                start_ms=bin_start,
                window_ms=config.bin_ms,
                open=price_f,
                high=price_f,
                low=price_f,
                close=price_f,
                volume=size_f,
                notional=price_f * size_f,
                trade_count=1,
            )
            symbol_bins[tick_symbol][bin_start] = state
        else:
            state.high = max(state.high, price_f)
            state.low = min(state.low, price_f)
            state.close = price_f
            state.volume += size_f
            state.notional += price_f * size_f
            state.trade_count += 1

        if lateness > state.max_lateness_ms:
            state.max_lateness_ms = int(lateness)

        flush_threshold = watermarks[tick_symbol] - config.max_late_ms
        ready_bins = [start for start, b in symbol_bins[tick_symbol].items() if b.start_ms + b.window_ms <= flush_threshold]
        for bin_start_ms in sorted(ready_bins):
            bin_state = symbol_bins[tick_symbol].pop(bin_start_ms)
            emit_ts_ms = epoch_ms(time.time())
            payload = bin_state.to_payload(emit_ts_ms)
            maybe_sleep(config, last_emit_ts_ms, tick_symbol, bin_state.start_ms)
            emit_bin(config, producer, payload)

    # Flush remaining bins on shutdown
    for tick_symbol, bins in symbol_bins.items():
        for bin_state in sorted(bins.values(), key=lambda b: b.start_ms):
            emit_ts_ms = epoch_ms(time.time())
            payload = bin_state.to_payload(emit_ts_ms)
            maybe_sleep(config, last_emit_ts_ms, tick_symbol, bin_state.start_ms)
            emit_bin(config, producer, payload)


def maybe_sleep(config: ProducerConfig, last_emit: Dict[str, int], symbol: str, current_start_ms: int) -> None:
    speed = config.replay_speed
    if speed <= 0:
        last_emit[symbol] = current_start_ms
        return
    last = last_emit.get(symbol, 0)
    if last == 0:
        last_emit[symbol] = current_start_ms
        return
    delta_ms = current_start_ms - last
    if delta_ms <= 0:
        last_emit[symbol] = current_start_ms
        return
    sleep_for = (delta_ms / 1000.0) / speed
    if sleep_for > 0:
        time.sleep(sleep_for)
    last_emit[symbol] = current_start_ms


def emit_bin(config: ProducerConfig, producer: Optional[KafkaProducer], payload: Dict[str, object]) -> None:
    key = payload["bin_id"]  # type: ignore[index]
    print(json.dumps(payload, separators=(",", ":")))
    if producer is None:
        append_jsonl(config.log_path, payload)
        return
    try:
        producer.send(config.topic, key=key, value=payload)
    except KafkaError as exc:  # pragma: no cover - broker failures
        print(f"[replay] Kafka send failed: {exc}; writing to log", file=sys.stderr)
        append_jsonl(config.log_path, payload)


def main() -> None:
    config = load_config()
    producer = init_producer(config.brokers)
    try:
        process_stream(config, producer)
    finally:
        if producer is not None:
            try:
                producer.flush(timeout=5)
            except KafkaError as exc:  # pragma: no cover - broker failures
                print(f"[replay] Kafka flush failed: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
