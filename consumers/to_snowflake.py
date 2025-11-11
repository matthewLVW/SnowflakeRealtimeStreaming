#!/usr/bin/env python3
"""Stream ticks.agg.1s Kafka bars into Snowflake FACT_TICKS_1S (or a custom table)."""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import shlex
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from cryptography.hazmat.primitives import serialization  # type: ignore
from kafka import KafkaConsumer  # type: ignore
from kafka.errors import KafkaError, NoBrokersAvailable  # type: ignore
import snowflake.connector  # type: ignore

LOGGER = logging.getLogger("snowflake-1s-loader")


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
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = shlex.split(value)[0]
        os.environ.setdefault(key, value)


def load_private_key(path: Optional[str], b64_value: Optional[str] = None):
    key_bytes: Optional[bytes] = None
    if b64_value:
        try:
            key_bytes = base64.b64decode(b64_value)
        except (base64.binascii.Error, ValueError) as exc:
            raise RuntimeError("Failed to decode SNOW_PRIVATE_KEY_B64") from exc
    elif path:
        key_path = Path(path).expanduser()
        if not key_path.exists():
            raise FileNotFoundError(f"Snowflake private key not found: {key_path}")
        key_bytes = key_path.read_bytes()
    if key_bytes is None:
        return None
    try:
        return serialization.load_der_private_key(key_bytes, password=None)
    except ValueError:
        return serialization.load_pem_private_key(key_bytes, password=None)


@dataclass
class LoaderConfig:
    brokers: List[str]
    topic: str
    group_id: str
    table: str
    batch_size: int
    poll_timeout_ms: int
    snow_account: str
    snow_user: str
    snow_password: Optional[str]
    snow_private_key_path: Optional[str]
    snow_private_key_b64: Optional[str]
    snow_role: Optional[str]
    snow_warehouse: str
    snow_database: str
    snow_schema: str


def load_config() -> LoaderConfig:
    brokers = os.environ.get("KAFKA_BROKERS", "localhost:9092")
    topic = (
        os.environ.get("AGG_OUTPUT_TOPIC")
        or os.environ.get("KAFKA_TOPIC_AGG")
        or "ticks.agg.1s"
    )
    group_id = os.environ.get("SNOWFLAKE_1S_GROUP", "snowflake-1s-loader")
    table = os.environ.get("SNOW_TABLE") or "FACT_TICKS_1S"
    batch_size = int(os.environ.get("SNOWFLAKE_STREAM_BATCH_SIZE", "100"))
    poll_timeout_ms = int(os.environ.get("SNOWFLAKE_STREAM_POLL_MS", "1000"))

    required_env = {
        "SNOW_ACCOUNT": os.environ.get("SNOW_ACCOUNT"),
        "SNOW_USER": os.environ.get("SNOW_USER"),
        "SNOW_WAREHOUSE": os.environ.get("SNOW_WAREHOUSE"),
        "SNOW_DATABASE": os.environ.get("SNOW_DATABASE"),
        "SNOW_SCHEMA": os.environ.get("SNOW_SCHEMA"),
    }
    missing = [key for key, value in required_env.items() if not value]
    if missing:
        raise RuntimeError(f"Missing Snowflake configuration variables: {', '.join(missing)}")

    password = os.environ.get("SNOW_PASSWORD")
    private_key_path = os.environ.get("SNOW_PRIVATE_KEY_PATH")
    private_key_b64 = os.environ.get("SNOW_PRIVATE_KEY_B64")
    if not password and not private_key_path and not private_key_b64:
        raise RuntimeError(
            "Missing Snowflake authentication secret. Provide SNOW_PASSWORD or "
            "SNOW_PRIVATE_KEY_PATH / SNOW_PRIVATE_KEY_B64."
        )

    return LoaderConfig(
        brokers=[entry.strip() for entry in brokers.split(",") if entry.strip()],
        topic=topic,
        group_id=group_id,
        table=table,
        batch_size=max(batch_size, 1),
        poll_timeout_ms=max(poll_timeout_ms, 100),
        snow_account=required_env["SNOW_ACCOUNT"],
        snow_user=required_env["SNOW_USER"],
        snow_password=password,
        snow_private_key_path=private_key_path,
        snow_private_key_b64=private_key_b64,
        snow_role=os.environ.get("SNOW_ROLE"),
        snow_warehouse=required_env["SNOW_WAREHOUSE"],
        snow_database=required_env["SNOW_DATABASE"],
        snow_schema=required_env["SNOW_SCHEMA"],
    )


def build_consumer(config: LoaderConfig) -> KafkaConsumer:
    try:
        consumer = KafkaConsumer(
            config.topic,
            bootstrap_servers=config.brokers or ["localhost:9092"],
            group_id=config.group_id,
            value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
            enable_auto_commit=True,
            auto_offset_reset="latest",
            consumer_timeout_ms=config.poll_timeout_ms,
        )
    except NoBrokersAvailable as exc:
        raise RuntimeError(f"Kafka unavailable at {','.join(config.brokers)}") from exc
    return consumer


class SnowflakeStreamWriter:
    def __init__(self, config: LoaderConfig) -> None:
        self.config = config
        self._connection: Optional[snowflake.connector.SnowflakeConnection] = None
        self._buffer: List[Tuple[object, ...]] = []
        self._minute_delete_sql = f"DELETE FROM {self.config.table} WHERE BAR_ID = %s"
        self._minute_insert_sql = (
            f"INSERT INTO {self.config.table} ("
            " BAR_ID, SYMBOL, START_TS, END_TS, OPEN, HIGH, LOW, CLOSE,"
            " VOLUME, NOTIONAL, VWAP, BIN_COUNT, MAX_LATENESS_MS, INGEST_TS"
            ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )

    def connect(self) -> None:
        kwargs = {
            "account": self.config.snow_account,
            "user": self.config.snow_user,
            "warehouse": self.config.snow_warehouse,
            "database": self.config.snow_database,
            "schema": self.config.snow_schema,
        }
        private_key = load_private_key(self.config.snow_private_key_path, self.config.snow_private_key_b64)
        if private_key is not None:
            kwargs["authenticator"] = "SNOWFLAKE_JWT"
            kwargs["private_key"] = private_key
        elif self.config.snow_password:
            kwargs["password"] = self.config.snow_password
        else:
            raise RuntimeError(
                "No Snowflake authentication secret available; configure SNOW_PASSWORD or private key variables."
            )
        if self.config.snow_role:
            kwargs["role"] = self.config.snow_role
        self._connection = snowflake.connector.connect(**kwargs)
        LOGGER.info(
            "Connected to Snowflake (warehouse=%s, db=%s, schema=%s, table=%s)",
            self.config.snow_warehouse,
            self.config.snow_database,
            self.config.snow_schema,
            self.config.table,
        )

    def append(self, row: Tuple[object, ...]) -> None:
        if self._connection is None:
            raise RuntimeError("Snowflake connection not initialized")
        self._buffer.append(row)
        if len(self._buffer) >= self.config.batch_size:
            self.flush()

    def flush(self) -> None:
        if self._connection is None or not self._buffer:
            return
        deletes = [(row[0],) for row in self._buffer]
        with self._connection.cursor() as cursor:
            cursor.executemany(self._minute_delete_sql, deletes)
            cursor.executemany(self._minute_insert_sql, self._buffer)
        self._connection.commit()
        LOGGER.info("Upserted %s 1s bars into %s", len(self._buffer), self.config.table)
        self._buffer.clear()

    def close(self) -> None:
        try:
            self.flush()
        finally:
            if self._connection is not None:
                try:
                    self._connection.close()
                finally:
                    self._connection = None


def _coerce_int(value: object) -> Optional[int]:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value))
        except ValueError:
            return None
    return None


def _coerce_float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return default
    return default


def record_to_row(record: Dict[str, object]) -> Optional[Tuple[object, ...]]:
    try:
        bar_id = str(record["bar_id"])
        symbol = str(record["symbol"])
    except KeyError:
        LOGGER.debug("Skipping record without bar_id/symbol: %s", record)
        return None
    start_ts_ms = _coerce_int(record.get("start_ts_ms"))
    end_ts_ms = _coerce_int(record.get("end_ts_ms"))
    ingest_ts_ms = _coerce_int(record.get("ingest_ts_ms")) or int(time.time() * 1000)
    if not start_ts_ms or not end_ts_ms:
        LOGGER.debug("Skipping record with missing timestamps: %s", record)
        return None
    start_dt = datetime.utcfromtimestamp(start_ts_ms / 1000.0).replace(tzinfo=timezone.utc)
    end_dt = datetime.utcfromtimestamp(end_ts_ms / 1000.0).replace(tzinfo=timezone.utc)
    ingest_dt = datetime.utcfromtimestamp(ingest_ts_ms / 1000.0).replace(tzinfo=timezone.utc)

    open_px = _coerce_float(record.get("open"))
    high_px = _coerce_float(record.get("high"), open_px)
    low_px = _coerce_float(record.get("low"), open_px)
    close_px = _coerce_float(record.get("close"), open_px)
    volume = _coerce_float(record.get("volume"))
    notional = _coerce_float(record.get("notional"))
    vwap = _coerce_float(record.get("vwap"), close_px)
    bin_count = _coerce_int(record.get("bin_count")) or 0
    max_lateness = _coerce_int(record.get("max_lateness_ms")) or 0

    return (
        bar_id,
        symbol,
        start_dt,
        end_dt,
        open_px,
        high_px,
        low_px,
        close_px,
        volume,
        notional,
        vwap,
        bin_count,
        max_lateness,
        ingest_dt,
    )


def run_loader(config: LoaderConfig) -> None:
    consumer = build_consumer(config)
    writer = SnowflakeStreamWriter(config)
    writer.connect()
    try:
        while True:
            try:
                polled = consumer.poll(timeout_ms=config.poll_timeout_ms, max_records=500)
            except KafkaError as exc:
                LOGGER.warning("Kafka poll failed: %s", exc)
                time.sleep(1.0)
                continue

            if not polled:
                writer.flush()
                continue

            for partition_records in polled.values():
                for message in partition_records:
                    value = message.value
                    if not isinstance(value, dict):
                        LOGGER.debug("Skipping non-dict payload: %s", value)
                        continue
                    row = record_to_row(value)
                    if row is None:
                        continue
                    writer.append(row)
    except KeyboardInterrupt:
        LOGGER.info("Stopping Snowflake 1s loader...")
    finally:
        writer.close()
        consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="Path to env file (default: .env)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    env_path = Path(args.env)
    parse_env_file(env_path)
    config = load_config()
    run_loader(config)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover - entry guard
        LOGGER.error("Snowflake loader failed: %s", exc)
        sys.exit(1)
