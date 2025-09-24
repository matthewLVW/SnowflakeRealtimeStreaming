#!/usr/bin/env python3
"""Convenience launcher for the full local pipeline (API ? Kafka ? dashboard ? Snowflake loader)."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class ProcessSpec:
    name: str
    command: List[str]
    cwd: Path
    optional: bool = False
    env_overrides: Optional[Dict[str, str]] = None


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
            value = value[1:-1]
        os.environ.setdefault(key, value)


def ensure_env(path: Path) -> None:
    parse_env_file(path)
    required = [
        "KAFKA_BROKERS",
        "MOCK_API_URL",
        "AGG_OUTPUT_TOPIC",
    ]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


def have_snowflake_config() -> bool:
    required = [
        "SNOW_ACCOUNT",
        "SNOW_USER",
        "SNOW_PASSWORD",
        "SNOW_WAREHOUSE",
        "SNOW_DATABASE",
        "SNOW_SCHEMA",
    ]
    return all(os.environ.get(key) for key in required)


def resolve_api_script(repo_root: Path, api_choice: str) -> Path:
    mapping = {
        "legacy": repo_root / "api" / "mock_tick_api.py",
        "realtime": repo_root / "api" / "mock_tick_api_realtime.py",
    }
    try:
        script = mapping[api_choice]
    except KeyError as exc:  # pragma: no cover - guarded by argparse choices
        raise ValueError(f"Unknown API choice: {api_choice}") from exc
    if not script.exists():
        raise FileNotFoundError(f"API script not found: {script}")
    return script


def build_process_plan(
    env_path: Path,
    include_loader: bool,
    api_choice: str,
    stream_speed: Optional[str],
) -> List[ProcessSpec]:
    repo_root = env_path.parent
    api_script = resolve_api_script(repo_root, api_choice)
    api_env: Dict[str, str] = {}
    if stream_speed is not None:
        api_env["MOCK_STREAM_SPEED"] = stream_speed

    specs: List[ProcessSpec] = [
        ProcessSpec(
            name="mock-api",
            command=[sys.executable, str(api_script)],
            cwd=repo_root,
            env_overrides=api_env or None,
        ),
        ProcessSpec(
            name="replay",
            command=[sys.executable, "ops/run_replay.py", "--env", str(env_path)],
            cwd=repo_root,
        ),
        ProcessSpec(
            name="aggregate-1s",
            command=[sys.executable, "processors/aggregate_1s.py", "--env", str(env_path)],
            cwd=repo_root,
        ),
    ]
    if include_loader:
        if have_snowflake_config():
            specs.append(
                ProcessSpec(
                    name="snowflake-loader",
                    command=[sys.executable, "consumers/snowflake_loader_minute.py", "--env", str(env_path)],
                    cwd=repo_root,
                    optional=True,
                )
            )
        else:
            print(
                "[pipeline] Snowflake credentials not set; skipping loader. Populate SNOW_ACCOUNT/SNOW_USER/etc to enable."
            )
    return specs


def start_process(spec: ProcessSpec, base_env: Dict[str, str]) -> subprocess.Popen[str]:
    env = base_env.copy()
    if spec.env_overrides:
        env.update(spec.env_overrides)
    return subprocess.Popen(
        spec.command,
        cwd=str(spec.cwd),
        env=env,
        stdout=None,
        stderr=None,
        text=True,
    )


def run_pipeline(
    env_path: Path,
    include_loader: bool,
    api_choice: str,
    stream_speed: Optional[str],
) -> None:
    ensure_env(env_path)
    base_env = os.environ.copy()
    plan = build_process_plan(env_path, include_loader, api_choice, stream_speed)
    processes: List[subprocess.Popen[str]] = []
    try:
        for spec in plan:
            try:
                proc = start_process(spec, base_env)
            except FileNotFoundError as exc:
                if spec.optional:
                    print(f"[pipeline] Skipping {spec.name} ({exc})")
                    continue
                raise
            processes.append(proc)
            print(f"[pipeline] Started {spec.name} (pid={proc.pid})")
            time.sleep(1.0)
        print("[pipeline] All processes running. Press Ctrl+C to stop.")
        while True:
            if all(proc.poll() is not None for proc in processes):
                break
            time.sleep(2.0)
    except KeyboardInterrupt:
        print("\n[pipeline] Caught interrupt, shutting down...")
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.send_signal(signal.SIGINT)
        time.sleep(2.0)
        for proc in processes:
            if proc.poll() is None:
                proc.terminate()
        for proc in processes:
            if proc.poll() is None:
                proc.kill()
        for proc in processes:
            proc.wait()
        print("[pipeline] All subprocesses stopped.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="Path to env file")
    parser.add_argument(
        "--no-loader",
        action="store_true",
        help="Skip starting the Snowflake micro-batch loader",
    )
    parser.add_argument(
        "--api",
        choices=["legacy", "realtime"],
        default="legacy",
        help="Which mock API to run (default: legacy)",
    )
    parser.add_argument(
        "--stream-speed",
        help="Override MOCK_STREAM_SPEED for the mock API process",
    )
    args = parser.parse_args()

    env_path = Path(args.env).resolve()
    if not env_path.exists():
        raise SystemExit(f"Env file not found: {env_path}")

    run_pipeline(
        env_path,
        include_loader=not args.no_loader,
        api_choice=args.api,
        stream_speed=args.stream_speed,
    )


if __name__ == "__main__":
    main()
