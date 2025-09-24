#!/usr/bin/env python3
"""Consume 1 s bars, build 1 min snapshots, and load micro-batches into Snowflake."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional, Sequence, Tuple

from kafka import KafkaConsumer  # type: ignore
from kafka.errors import KafkaError, NoBrokersAvailable  # type: ignore
import snowflake.connector  # type: ignore

LOGGER = logging.getLogger("snowflake-loader")


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
        if (
            (value.startswith("\"") and value.endswith("\""))
            or (value.startswith("'") and value.endswith("'"))
        ):
            value = shlex.split(value)[0]
        os.environ.setdefault(key, value)


@dataclass
class LoaderConfig:
    brokers: str
    topic: str
    group_id: str
    flush_minutes: int
    poll_timeout_ms: int
    idle_settle_ms: int
    minute_table: str
    five_minute_table: Optional[str]
    snow_account: str
    snow_user: str
    snow_password: str
    snow_role: Optional[str]
    snow_warehouse: str
    snow_database: str
    snow_schema: str

    @property
    def bootstrap_servers(self) -> List[str]:
        return [entry.strip() for entry in self.brokers.split(",") if entry.strip()]


@dataclass
class MinuteAccumulator:
    symbol: str
    start_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    notional: float
    ingest_ts_ms: int
    count: int = 1

    def update(self, bar: Dict[str, object]) -> None:
        high_px = float(bar.get("high", self.high))
        low_px = float(bar.get("low", self.low))
        close_px = float(bar.get("close", self.close))
        volume = float(bar.get("volume", 0.0))
        notional = float(bar.get("notional", close_px * volume))
        ingest_ts = int(bar.get("ingest_ts_ms", self.ingest_ts_ms))

        if high_px > self.high:
            self.high = high_px
        if low_px < self.low:
            self.low = low_px
        self.close = close_px
        self.volume += volume
        self.notional += notional
        if ingest_ts > self.ingest_ts_ms:
            self.ingest_ts_ms = ingest_ts
        self.count += 1

    def to_minute_bar(self) -> MinuteBar:
        end_ms = self.start_ms + 60_000
        return MinuteBar(
            symbol=self.symbol,
            start_ms=self.start_ms,
            end_ms=end_ms,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            notional=self.notional,
            ingest_ts_ms=self.ingest_ts_ms,
            source_count=self.count,
        )


@dataclass
class MinuteBar:
    symbol: str
    start_ms: int
    end_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    notional: float
    ingest_ts_ms: int
    source_count: int

    def bar_id(self, suffix: str = "60s") -> str:
        return f"{self.symbol}-{self.start_ms:013d}-{suffix}"

    def to_row(self, minute_index: int) -> Tuple[object, ...]:
        start_dt = datetime.utcfromtimestamp(self.start_ms / 1000.0).replace(tzinfo=timezone.utc)
        end_dt = datetime.utcfromtimestamp(self.end_ms / 1000.0).replace(tzinfo=timezone.utc)
        ingest_dt = datetime.utcfromtimestamp(self.ingest_ts_ms / 1000.0).replace(tzinfo=timezone.utc)
        return (
            self.bar_id(),
            self.symbol,
            start_dt,
            end_dt,
            self.open,
            self.close,
            self.high,
            self.low,
            self.volume,
            self.notional,
            ingest_dt,
            self.source_count,
            minute_index,
        )


def floor_minute(ts_ms: int) -> int:
    return ts_ms - (ts_ms % 60_000)


class MinuteAggregator:
    def __init__(self, flush_minutes: int) -> None:
        self.flush_minutes = flush_minutes
        self._active: Dict[str, Dict[int, MinuteAccumulator]] = defaultdict(dict)
        self._pending: Dict[str, Deque[MinuteBar]] = defaultdict(deque)

    def ingest(self, bar: Dict[str, object]) -> None:
        symbol = str(bar.get("symbol") or "UNKNOWN")
        start_ts = int(bar.get("start_ts_ms", 0))
        if start_ts <= 0:
            return
        minute_start = floor_minute(start_ts)
        acc = self._active[symbol].get(minute_start)
        if acc is None:
            open_px = float(bar.get("open", bar.get("close", 0.0)))
            high_px = float(bar.get("high", open_px))
            low_px = float(bar.get("low", open_px))
            close_px = float(bar.get("close", open_px))
            volume = float(bar.get("volume", 0.0))
            notional = float(bar.get("notional", close_px * volume))
            ingest_ts = int(bar.get("ingest_ts_ms", int(time.time() * 1000)))
            acc = MinuteAccumulator(
                symbol=symbol,
                start_ms=minute_start,
                open=open_px,
                high=high_px,
                low=low_px,
                close=close_px,
                volume=volume,
                notional=notional,
                ingest_ts_ms=ingest_ts,
            )
            self._active[symbol][minute_start] = acc
        else:
            acc.update(bar)
        self._finalize_completed(symbol, minute_start)

    def _finalize_completed(self, symbol: str, current_minute: int) -> None:
        minutes = self._active[symbol]
        ready_keys = [minute for minute in minutes.keys() if minute < current_minute]
        for minute in sorted(ready_keys):
            acc = minutes.pop(minute)
            self._pending[symbol].append(acc.to_minute_bar())

    def settle_idle_minutes(self, threshold_ms: int) -> None:
        for symbol, minutes in list(self._active.items()):
            ready_keys = [
                minute
                for minute, acc in minutes.items()
                if (acc.start_ms + 60_000) <= threshold_ms
            ]
            for minute in sorted(ready_keys):
                acc = minutes.pop(minute)
                self._pending[symbol].append(acc.to_minute_bar())
            if not minutes:
                self._active.pop(symbol, None)

    def pop_ready_batches(self) -> List[Tuple[str, List[MinuteBar]]]:
        batches: List[Tuple[str, List[MinuteBar]]] = []
        for symbol, queue in self._pending.items():
            while len(queue) >= self.flush_minutes:
                batch = [queue.popleft() for _ in range(self.flush_minutes)]
                batches.append((symbol, batch))
        return batches

    def drain_all(self) -> List[Tuple[str, List[MinuteBar]]]:
        for symbol, minutes in list(self._active.items()):
            for acc in minutes.values():
                self._pending[symbol].append(acc.to_minute_bar())
            self._active.pop(symbol, None)
        batches: List[Tuple[str, List[MinuteBar]]] = []
        for symbol, queue in list(self._pending.items()):
            if queue:
                batches.append((symbol, list(queue)))
                queue.clear()
        return batches


class SnowflakeLoader:
    def __init__(self, config: LoaderConfig) -> None:
        self.config = config
        self._connection: Optional[snowflake.connector.SnowflakeConnection] = None
        self._minute_insert_sql = (
            f"INSERT INTO {self.config.minute_table} ("
            " BAR_ID, SYMBOL, START_TS, END_TS, OPEN, CLOSE, HIGH, LOW, VOLUME, NOTIONAL, INGEST_TS, SOURCE_COUNT, MINUTE_INDEX"
            ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        self._minute_delete_sql = f"DELETE FROM {self.config.minute_table} WHERE BAR_ID = %s"
        if self.config.five_minute_table:
            self._five_minute_insert_sql = (
                f"INSERT INTO {self.config.five_minute_table} ("
                " BAR_ID, SYMBOL, START_TS, END_TS, OPEN, CLOSE, HIGH, LOW, VOLUME, NOTIONAL, INGEST_TS, MINUTE_COUNT, SOURCE_COUNT"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
            self._five_minute_delete_sql = f"DELETE FROM {self.config.five_minute_table} WHERE BAR_ID = %s"
        else:
            self._five_minute_insert_sql = None
            self._five_minute_delete_sql = None

    def connect(self) -> None:
        kwargs = {
            "account": self.config.snow_account,
            "user": self.config.snow_user,
            "password": self.config.snow_password,
            "warehouse": self.config.snow_warehouse,
            "database": self.config.snow_database,
            "schema": self.config.snow_schema,
        }
        if self.config.snow_role:
            kwargs["role"] = self.config.snow_role
        self._connection = snowflake.connector.connect(**kwargs)
        LOGGER.info("Connected to Snowflake (warehouse=%s, db=%s, schema=%s)", self.config.snow_warehouse, self.config.snow_database, self.config.snow_schema)

    def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None

    def flush_batch(self, symbol: str, batch: List[MinuteBar]) -> None:
        if not batch or self._connection is None:
            return
        minute_rows = [bar.to_row(idx) for idx, bar in enumerate(batch)]
        minute_deletes = [(row[0],) for row in minute_rows]
        with self._connection.cursor() as cursor:
            cursor.executemany(self._minute_delete_sql, minute_deletes)
            cursor.executemany(self._minute_insert_sql, minute_rows)
        if self._five_minute_insert_sql and len(batch) == self.config.flush_minutes:
            five_row = build_five_minute_row(symbol, batch)
            with self._connection.cursor() as cursor:
                cursor.execute(self._five_minute_delete_sql, (five_row[0],))
                cursor.execute(self._five_minute_insert_sql, five_row)
        self._connection.commit()
        LOGGER.info(
            "Upserted %s minute rows (%s) and %s 5 min rows", len(batch), symbol, 1 if self._five_minute_insert_sql else 0
        )


def build_five_minute_row(symbol: str, batch: List[MinuteBar]) -> Tuple[object, ...]:
    start_ms = batch[0].start_ms
    end_ms = batch[-1].end_ms
    open_px = batch[0].open
    close_px = batch[-1].close
    high_px = max(bar.high for bar in batch)
    low_px = min(bar.low for bar in batch)
    volume = sum(bar.volume for bar in batch)
    notional = sum(bar.notional for bar in batch)
    ingest_ts_ms = max(bar.ingest_ts_ms for bar in batch)

    start_dt = datetime.utcfromtimestamp(start_ms / 1000.0).replace(tzinfo=timezone.utc)
    end_dt = datetime.utcfromtimestamp(end_ms / 1000.0).replace(tzinfo=timezone.utc)
    ingest_dt = datetime.utcfromtimestamp(ingest_ts_ms / 1000.0).replace(tzinfo=timezone.utc)
    source_count = sum(bar.source_count for bar in batch)
    bar_id = f"{symbol}-{start_ms:013d}-300s"

    return (
        bar_id,
        symbol,
        start_dt,
        end_dt,
        open_px,
        close_px,
        high_px,
        low_px,
        volume,
        notional,
        ingest_dt,
        len(batch),
        source_count,
    )



def build_consumer(config: LoaderConfig) -> KafkaConsumer:
    try:
        consumer = KafkaConsumer(
            config.topic,
            bootstrap_servers=config.bootstrap_servers or ["localhost:9092"],
            group_id=config.group_id,
            value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
            enable_auto_commit=True,
            auto_offset_reset="latest",
            consumer_timeout_ms=config.poll_timeout_ms,
        )
    except NoBrokersAvailable as exc:
        raise RuntimeError(f"Kafka unavailable at {config.brokers}") from exc
    return consumer


def load_config() -> LoaderConfig:
    brokers = os.environ.get("KAFKA_BROKERS", "localhost:9092")
    topic = os.environ.get("AGG_OUTPUT_TOPIC", "ticks.agg.1s")
    group_id = os.environ.get("SNOWFLAKE_LOADER_GROUP", "snowflake-minute-loader")
    flush_minutes = int(os.environ.get("SNOWFLAKE_FLUSH_MINUTES", "5"))
    poll_timeout_ms = int(os.environ.get("SNOWFLAKE_POLL_TIMEOUT_MS", "1000"))
    idle_settle_ms = int(os.environ.get("SNOWFLAKE_IDLE_SETTLE_MS", "120000"))
    minute_table = os.environ.get("SNOWFLAKE_MINUTE_TABLE", "FACT_TICKS_60S")
    five_minute_table = os.environ.get("SNOWFLAKE_FIVEM_TABLE") or None

    required_env = {
        "SNOW_ACCOUNT": os.environ.get("SNOW_ACCOUNT"),
        "SNOW_USER": os.environ.get("SNOW_USER"),
        "SNOW_PASSWORD": os.environ.get("SNOW_PASSWORD"),
        "SNOW_WAREHOUSE": os.environ.get("SNOW_WAREHOUSE"),
        "SNOW_DATABASE": os.environ.get("SNOW_DATABASE"),
        "SNOW_SCHEMA": os.environ.get("SNOW_SCHEMA"),
    }
    missing = [key for key, value in required_env.items() if not value]
    if missing:
        raise RuntimeError(f"Missing Snowflake configuration variables: {', '.join(missing)}")

    return LoaderConfig(
        brokers=brokers,
        topic=topic,
        group_id=group_id,
        flush_minutes=flush_minutes,
        poll_timeout_ms=poll_timeout_ms,
        idle_settle_ms=idle_settle_ms,
        minute_table=minute_table,
        five_minute_table=five_minute_table,
        snow_account=required_env["SNOW_ACCOUNT"],
        snow_user=required_env["SNOW_USER"],
        snow_password=required_env["SNOW_PASSWORD"],
        snow_role=os.environ.get("SNOW_ROLE"),
        snow_warehouse=required_env["SNOW_WAREHOUSE"],
        snow_database=required_env["SNOW_DATABASE"],
        snow_schema=required_env["SNOW_SCHEMA"],
    )


def run_loader(config: LoaderConfig, env_path: Path) -> None:
    consumer = build_consumer(config)
    loader = SnowflakeLoader(config)
    aggregator = MinuteAggregator(config.flush_minutes)
    loader.connect()
    last_settle_ms = int(time.time() * 1000)
    try:
        while True:
            try:
                records = consumer.poll(timeout_ms=config.poll_timeout_ms, max_records=500)
            except KafkaError as exc:
                LOGGER.warning("Kafka poll failed: %s", exc)
                time.sleep(1.0)
                continue
            now_ms = int(time.time() * 1000)
            if records:
                for messages in records.values():
                    for message in messages:
                        value = message.value
                        if isinstance(value, dict):
                            aggregator.ingest(value)
            if now_ms - last_settle_ms >= config.idle_settle_ms:
                aggregator.settle_idle_minutes(now_ms - 2_000)
                last_settle_ms = now_ms
            batches = aggregator.pop_ready_batches()
            for symbol, batch in batches:
                loader.flush_batch(symbol, batch)
    except KeyboardInterrupt:
        LOGGER.info("Shutting down loader on interrupt")
        remaining = aggregator.drain_all()
        for symbol, batch in remaining:
            loader.flush_batch(symbol, batch)
    finally:
        loader.close()
        consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="Env file to read (default: .env)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    parse_env_file(Path(args.env))
    config = load_config()
    run_loader(config, Path(args.env))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover - entry point guard
        LOGGER.error("Loader failed: %s", exc)
        sys.exit(1)





