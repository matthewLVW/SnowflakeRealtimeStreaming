"""Load env vars and launch the historical replay producer."""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
from pathlib import Path


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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="Env file to read (default: .env)")
    parser.add_argument("--default-topic", default="ticks.raw", help="Fallback Kafka topic")
    parser.add_argument("--api", help="Override MOCK_API_URL")
    parser.add_argument("--symbol", help="Symbol override (REPLAY_SYMBOL)")
    parser.add_argument("--bin-ms", help="Override REPLAY_BIN_MS")
    parser.add_argument("--late-ms", help="Override REPLAY_MAX_LATE_MS")
    parser.add_argument("--speed", help="Override REPLAY_SPEED")
    args = parser.parse_args()

    parse_env_file(Path(args.env))

    os.environ.setdefault("KAFKA_BROKERS", "localhost:9092")
    os.environ.setdefault("KAFKA_TOPIC", args.default_topic)

    if args.api:
        os.environ["MOCK_API_URL"] = args.api
    if args.symbol:
        os.environ["REPLAY_SYMBOL"] = args.symbol
    if args.bin_ms:
        os.environ["REPLAY_BIN_MS"] = args.bin_ms
    if args.late_ms:
        os.environ["REPLAY_MAX_LATE_MS"] = args.late_ms
    if args.speed:
        os.environ["REPLAY_SPEED"] = args.speed

    subprocess.run([
        "python",
        "producers/replay_producer.py",
    ], check=True)


if __name__ == "__main__":
    main()
