#!/usr/bin/env python3
"""Aggregate 10 ms bins into 1 s OHLCV bars."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from kafka import KafkaConsumer, KafkaProducer  # type: ignore
from kafka.errors import KafkaError, NoBrokersAvailable  # type: ignore


def parse_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if (value.startswith("\"") and value.endswith("\"")) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = shlex.split(value)[0]
        os.environ.setdefault(key, value)


@dataclass
class AggregatorConfig:
    brokers: str
    input_topic: str
    output_topic: str
    group_id: str
    max_late_ms: int
    id_key: str
    log_path: Path


@dataclass
class BarState:
    symbol: str
    start_ms: int
    end_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    notional: float
    bin_count: int
    max_lateness_ms: int = 0
    api_send_ts_ms: Optional[int] = None
    source_event_ts_ms: Optional[int] = None

    def to_payload(self, ingest_ts_ms: int) -> Dict[str, object]:
        bar_id = f"{self.symbol}-{self.start_ms:013d}"
        vwap = round(self.notional / self.volume, 6) if self.volume else self.close
        return {
            "bar_id": bar_id,
            "symbol": self.symbol,
            "start_ts_ms": self.start_ms,
            "end_ts_ms": self.end_ms,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": round(self.volume, 6),
            "notional": round(self.notional, 6),
            "vwap": round(vwap, 6),
            "bin_count": self.bin_count,
            "max_lateness_ms": self.max_lateness_ms,
            "api_send_ts_ms": self.api_send_ts_ms,
            "source_event_ts_ms": self.source_event_ts_ms,
            "ingest_ts_ms": ingest_ts_ms,
            "source": "aggregator",
        }


def load_config() -> AggregatorConfig:
    brokers = os.environ.get("KAFKA_BROKERS", "localhost:9092")
    input_topic = os.environ.get("AGG_INPUT_TOPIC", os.environ.get("KAFKA_TOPIC_RAW", "ticks.raw"))
    output_topic = os.environ.get("AGG_OUTPUT_TOPIC", "ticks.agg.1s")
    group_id = os.environ.get("AGG_GROUP_ID", "aggregate-1s")
    max_late_ms = int(os.environ.get("REPLAY_MAX_LATE_MS", "500"))
    id_key = os.environ.get("AGG_IDEMPOTENT_KEY", "bar_id")
    log_path = Path(os.environ.get("AGG_LOG", "logs/aggregate.jsonl"))
    return AggregatorConfig(
        brokers=brokers,
        input_topic=input_topic,
        output_topic=output_topic,
        group_id=group_id,
        max_late_ms=max_late_ms,
        id_key=id_key,
        log_path=log_path,
    )


def deserialize_value(blob: Optional[bytes]) -> Dict[str, object]:
    if blob is None:
        return {}
    text = blob.decode("utf-8").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        print(f"[agg] skipping malformed payload: {exc}", file=sys.stderr)
        return {}


def coerce_int_ms(value: object) -> Optional[int]:
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return None
    return None


def init_producer(brokers: str) -> Optional[KafkaProducer]:
    try:
        return KafkaProducer(
            bootstrap_servers=brokers.split(","),
            key_serializer=lambda v: v.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        )
    except NoBrokersAvailable:
        print("[agg] Kafka unavailable; logging to file only.", file=sys.stderr)
        return None


def init_consumer(config: AggregatorConfig) -> KafkaConsumer:
    return KafkaConsumer(
        config.input_topic,
        bootstrap_servers=config.brokers.split(","),
        group_id=config.group_id,
        value_deserializer=deserialize_value,
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )


def append_jsonl(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, separators=(",", ":")) + "\n")


def floor_second(ts_ms: int) -> int:
    return (ts_ms // 1000) * 1000


def process(
    config: AggregatorConfig,
    consumer: KafkaConsumer,
    producer: Optional[KafkaProducer],
    exit_on_idle: bool,
) -> None:
    bar_state: Dict[str, Dict[int, BarState]] = defaultdict(dict)
    watermarks: Dict[str, int] = defaultdict(int)

    try:
        while True:
            records = consumer.poll(timeout_ms=500)
            if not records:
                flush_ready_bars(config, producer, bar_state, watermarks, force=False)
                if exit_on_idle and not any(bar_state.values()):
                    break
                continue
            for msgs in records.values():
                for message in msgs:
                    tick = message.value or {}
                    if "open" not in tick or "start_ts_ms" not in tick:
                        continue
                    symbol = str(tick.get("symbol", "")) or "UNKNOWN"
                    start_ts = int(tick.get("start_ts_ms", 0))
                    end_ts = int(tick.get("end_ts_ms", start_ts + 10))
                    open_px = float(tick.get("open", 0))
                    high_px = float(tick.get("high", open_px))
                    low_px = float(tick.get("low", open_px))
                    close_px = float(tick.get("close", open_px))
                    volume = float(tick.get("volume", 0))
                    notional = float(tick.get("notional", 0))
                    api_send_ts_ms = coerce_int_ms(tick.get("api_send_ts_ms"))
                    source_event_ts_ms = coerce_int_ms(tick.get("source_event_ts_ms"))

                    watermark = watermarks[symbol]
                    if end_ts > watermark:
                        watermarks[symbol] = end_ts
                        lateness = 0
                    else:
                        lateness = watermark - end_ts

                    if lateness > config.max_late_ms:
                        append_jsonl(
                            config.log_path.parent / "aggregate_dlt.jsonl",
                            {
                                "reason": "late_bin",
                                "lateness_ms": lateness,
                                "symbol": symbol,
                                "start_ts_ms": start_ts,
                                "end_ts_ms": end_ts,
                                "payload": tick,
                            },
                        )
                        continue

                    bar_start = floor_second(start_ts)
                    bar_end = bar_start + 1000
                    state = bar_state[symbol].get(bar_start)
                    if state is None:
                        state = BarState(
                            symbol=symbol,
                            start_ms=bar_start,
                            end_ms=bar_end,
                            open=open_px,
                            high=high_px,
                            low=low_px,
                            close=close_px,
                            volume=volume,
                            notional=notional,
                            bin_count=1,
                            api_send_ts_ms=api_send_ts_ms,
                            source_event_ts_ms=source_event_ts_ms,
                        )
                        bar_state[symbol][bar_start] = state
                    else:
                        state.high = max(state.high, high_px)
                        state.low = min(state.low, low_px)
                        state.close = close_px
                        state.volume += volume
                        state.notional += notional
                        state.bin_count += 1
                    if api_send_ts_ms is not None:
                        if state.api_send_ts_ms is None or api_send_ts_ms < state.api_send_ts_ms:
                            state.api_send_ts_ms = api_send_ts_ms
                    if source_event_ts_ms is not None:
                        if state.source_event_ts_ms is None or source_event_ts_ms < state.source_event_ts_ms:
                            state.source_event_ts_ms = source_event_ts_ms
                    if lateness > state.max_lateness_ms:
                        state.max_lateness_ms = int(lateness)

            flush_ready_bars(config, producer, bar_state, watermarks, force=False)
    except KeyboardInterrupt:
        print("[agg] Shutting down on interrupt.")
    finally:
        flush_ready_bars(config, producer, bar_state, watermarks, force=True)


def flush_ready_bars(
    config: AggregatorConfig,
    producer: Optional[KafkaProducer],
    bar_state: Dict[str, Dict[int, BarState]],
    watermarks: Dict[str, int],
    force: bool,
) -> None:
    for symbol, bars in bar_state.items():
        watermark = watermarks[symbol]
        threshold = watermark - config.max_late_ms
        ready = []
        for start_ms, state in bars.items():
            if force or state.end_ms <= threshold:
                ready.append(start_ms)
        for start_ms in sorted(ready):
            state = bars.pop(start_ms)
            payload = state.to_payload(int(time.time() * 1000))
            emit_bar(config, producer, payload)


def emit_bar(config: AggregatorConfig, producer: Optional[KafkaProducer], payload: Dict[str, object]) -> None:
    key = payload.get(config.id_key) or payload.get("bar_id")
    key_str = str(key)
    print(json.dumps(payload, separators=(",", ":")))
    if producer is None:
        append_jsonl(config.log_path, payload)
        return
    try:
        producer.send(config.output_topic, key=key_str, value=payload)
    except KafkaError as exc:  # pragma: no cover - broker failures
        print(f"[agg] Kafka send failed: {exc}; logging locally", file=sys.stderr)
        append_jsonl(config.log_path, payload)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="Env file to read (default: .env)")
    parser.add_argument("--exit-on-idle", action="store_true", help="Exit once topics are idle and buffers are flushed")
    args = parser.parse_args()

    parse_env_file(Path(args.env))
    config = load_config()
    producer = init_producer(config.brokers)
    consumer = init_consumer(config)
    try:
        process(config, consumer, producer, args.exit_on_idle)
    finally:
        consumer.close()
        if producer is not None:
            try:
                producer.flush(timeout=5)
            except KafkaError as exc:  # pragma: no cover - broker failures
                print(f"[agg] Kafka flush failed: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
