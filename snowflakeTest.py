#!/usr/bin/env python3
"""Sanity-check Snowflake key-pair authentication."""

from __future__ import annotations

import argparse
import base64
import os
import shlex
from pathlib import Path
from typing import Optional

import snowflake.connector  # type: ignore
from cryptography.hazmat.primitives import serialization


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
            (value.startswith('"') and value.endswith('"'))
            or (value.startswith("'") and value.endswith("'"))
        ):
            value = shlex.split(value)[0]
        os.environ.setdefault(key, value)


def load_private_key_bytes() -> bytes:
    b64_value = os.environ.get("SNOW_PRIVATE_KEY_B64")
    if b64_value:
        return base64.b64decode(b64_value)
    path_value = os.environ.get("SNOW_PRIVATE_KEY_PATH")
    if not path_value:
        raise RuntimeError("Set SNOW_PRIVATE_KEY_B64 or SNOW_PRIVATE_KEY_PATH")
    key_path = Path(path_value).expanduser()
    if not key_path.exists():
        raise FileNotFoundError(f"Private key not found: {key_path}")
    return key_path.read_bytes()


def load_private_key() -> serialization.PrivateFormat:
    private_bytes = load_private_key_bytes()
    return serialization.load_der_private_key(private_bytes, password=None)


def build_connection():
    required = [
        "SNOW_ACCOUNT",
        "SNOW_USER",
        "SNOW_WAREHOUSE",
        "SNOW_DATABASE",
        "SNOW_SCHEMA",
    ]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

    private_key = load_private_key()
    return snowflake.connector.connect(
        account=os.environ["SNOW_ACCOUNT"],
        user=os.environ["SNOW_USER"],
        authenticator="SNOWFLAKE_JWT",
        private_key=private_key,
        warehouse=os.environ["SNOW_WAREHOUSE"],
        database=os.environ["SNOW_DATABASE"],
        schema=os.environ["SNOW_SCHEMA"],
        role=os.environ.get("SNOW_ROLE"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="Path to .env file (default: .env)")
    args = parser.parse_args()

    parse_env_file(Path(args.env))
    conn = build_connection()
    try:
        user, role = conn.cursor().execute("select current_user(), current_role()").fetchone()
        print(f"Authenticated as {user} using role {role}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
