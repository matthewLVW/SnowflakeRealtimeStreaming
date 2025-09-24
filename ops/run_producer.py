"""Load env vars and launch the yfinance producer.

Allows running `make producer` on Windows where POSIX shell features are
unavailable. Minimal .env parser: ignores comments and blank lines.
"""

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
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        # Allow quoted values to preserve spaces
        if (value.startswith("\"") and value.endswith("\"")) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = shlex.split(value)[0]
        os.environ.setdefault(key, value)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="Env file to read (default: .env)")
    parser.add_argument(
        "--default-topic",
        default="ticks.raw",
        help="Topic to use if KAFKA_TOPIC not set",
    )
    args = parser.parse_args()

    parse_env_file(Path(args.env))

    os.environ.setdefault("KAFKA_BROKERS", "localhost:9092")
    os.environ.setdefault("KAFKA_TOPIC", args.default_topic)

    subprocess.run([
        "python",
        "producers/yfinance_producer.py",
    ], check=True)


if __name__ == "__main__":
    main()
