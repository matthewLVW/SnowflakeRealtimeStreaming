from __future__ import annotations

import json
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional

from kafka import KafkaConsumer  # type: ignore


@dataclass
class CacheConfig:
    """Runtime configuration for the in-memory cache consumer."""

    brokers: str
    topic: str
    group_id: str
    max_per_symbol: int = 600
    poll_timeout_ms: int = 500
    idle_sleep_s: float = 0.05


def _parse_brokers(raw: str) -> List[str]:
    return [entry.strip() for entry in raw.split(",") if entry.strip()]


def build_consumer(config: CacheConfig) -> KafkaConsumer:
    return KafkaConsumer(
        config.topic,
        bootstrap_servers=_parse_brokers(config.brokers) or ["localhost:9092"],
        group_id=config.group_id,
        value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="latest",
        consumer_timeout_ms=config.poll_timeout_ms,
    )


class BarCache:
    """Thread-safe in-memory store of recent 1 s bars and latency samples."""

    def __init__(self, max_per_symbol: int = 600, metrics_window: int = 120) -> None:
        self._bars: Dict[str, Deque[Dict[str, object]]] = defaultdict(
            lambda: deque(maxlen=max_per_symbol)
        )
        self._lock = threading.Lock()
        self._latencies = deque(maxlen=metrics_window)
        self._freshness = deque(maxlen=metrics_window)
        self._log_lag = deque(maxlen=metrics_window)
        self._last_latency_ms = 0.0
        self._last_freshness_ms = 0.0
        self._last_log_lag_ms = 0.0
        self._latest_end_ts_ms = 0
        self._latest_ingest_ts_ms = 0
        self._last_update_ts_ms = 0
        self._errors: Deque[str] = deque(maxlen=20)

    def ingest(
        self,
        bar: Dict[str, object],
        received_ts_ms: int,
        record_timestamp_ms: Optional[int] = None,
    ) -> None:
        symbol = str(bar.get("symbol", "UNKNOWN"))
        start_ts_ms = int(bar.get("start_ts_ms", 0))
        end_ts_ms = int(bar.get("end_ts_ms", start_ts_ms + 1000))
        ingest_ts_ms = int(bar.get("ingest_ts_ms", received_ts_ms))
        payload = {
            "bar_id": bar.get("bar_id"),
            "symbol": symbol,
            "start_ts_ms": start_ts_ms,
            "end_ts_ms": end_ts_ms,
            "open": float(bar.get("open", 0.0)),
            "high": float(bar.get("high", 0.0)),
            "low": float(bar.get("low", 0.0)),
            "close": float(bar.get("close", 0.0)),
            "volume": float(bar.get("volume", 0.0)),
            "notional": float(bar.get("notional", 0.0)),
            "vwap": float(bar.get("vwap", 0.0)),
            "bin_count": int(bar.get("bin_count", 0)),
            "max_lateness_ms": int(bar.get("max_lateness_ms", 0)),
            "ingest_ts_ms": ingest_ts_ms,
            "source": bar.get("source", "aggregator"),
        }

        latency_ms = max(ingest_ts_ms - end_ts_ms, 0)
        freshness_ms = max(received_ts_ms - end_ts_ms, 0)
        log_lag_ms = (
            max(received_ts_ms - record_timestamp_ms, 0)
            if record_timestamp_ms is not None
            else 0
        )

        with self._lock:
            self._bars[symbol].append(payload)
            self._latencies.append(latency_ms)
            self._freshness.append(freshness_ms)
            if record_timestamp_ms is not None:
                self._log_lag.append(log_lag_ms)
                self._last_log_lag_ms = float(log_lag_ms)
            self._last_latency_ms = float(latency_ms)
            self._last_freshness_ms = float(freshness_ms)
            self._latest_end_ts_ms = max(self._latest_end_ts_ms, end_ts_ms)
            self._latest_ingest_ts_ms = max(self._latest_ingest_ts_ms, ingest_ts_ms)
            self._last_update_ts_ms = received_ts_ms

    def record_error(self, message: str) -> None:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        with self._lock:
            self._errors.appendleft(f"[{timestamp}] {message}")

    def snapshot(self, symbol: Optional[str] = None) -> Dict[str, List[Dict[str, object]]]:
        with self._lock:
            if symbol is not None:
                return {symbol: list(self._bars.get(symbol, []))}
            return {key: list(deque_items) for key, deque_items in self._bars.items()}

    def metrics(self) -> Dict[str, float]:
        now_ms = int(time.time() * 1000)
        with self._lock:
            avg_latency = sum(self._latencies) / len(self._latencies) if self._latencies else 0.0
            avg_freshness = sum(self._freshness) / len(self._freshness) if self._freshness else 0.0
            avg_log_lag = sum(self._log_lag) / len(self._log_lag) if self._log_lag else 0.0
            backlog_ms = max(now_ms - self._latest_end_ts_ms, 0)
            bars_cached = sum(len(items) for items in self._bars.values())
            return {
                "last_latency_ms": self._last_latency_ms,
                "avg_latency_ms": float(avg_latency),
                "last_freshness_ms": self._last_freshness_ms,
                "avg_freshness_ms": float(avg_freshness),
                "last_log_lag_ms": self._last_log_lag_ms,
                "avg_log_lag_ms": float(avg_log_lag),
                "backlog_ms": float(backlog_ms),
                "bars_cached": float(bars_cached),
                "last_update_ts_ms": float(self._last_update_ts_ms),
            }

    def symbols(self) -> List[str]:
        with self._lock:
            return sorted(self._bars.keys())

    def errors(self) -> List[str]:
        with self._lock:
            return list(self._errors)


class CacheConsumer:
    """Background Kafka consumer that feeds the in-memory cache."""

    def __init__(self, config: CacheConfig, cache: Optional[BarCache] = None) -> None:
        self.config = config
        self.cache = cache or BarCache(max_per_symbol=config.max_per_symbol)
        self._consumer: Optional[KafkaConsumer] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._start_lock = threading.Lock()

    def start(self) -> BarCache:
        with self._start_lock:
            if self._thread and self._thread.is_alive():
                return self.cache
            if self._consumer is None:
                self._consumer = build_consumer(self.config)
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run, name="bar-cache", daemon=True)
            self._thread.start()
        return self.cache

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1.0)
        if self._consumer is not None:
            try:
                self._consumer.close()
            finally:
                self._consumer = None

    def _run(self) -> None:
        if self._consumer is None:
            return
        consumer = self._consumer
        while not self._stop_event.is_set():
            try:
                records = consumer.poll(timeout_ms=self.config.poll_timeout_ms, max_records=500)
            except Exception as exc:  # pragma: no cover - broker errors
                self.cache.record_error(f"poll failed: {exc}")
                time.sleep(1.0)
                continue
            if not records:
                time.sleep(self.config.idle_sleep_s)
                continue
            received_ts_ms = int(time.time() * 1000)
            for batch in records.values():
                for message in batch:
                    value = message.value
                    if not isinstance(value, dict):
                        continue
                    self.cache.ingest(
                        value,
                        received_ts_ms,
                        getattr(message, "timestamp", None),
                    )

    def __del__(self) -> None:
        try:
            self.stop()
        except Exception:
            pass

