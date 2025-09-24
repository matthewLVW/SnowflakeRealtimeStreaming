#!/usr/bin/env python3
"""Validate ticks.agg.1s bars against replayed 10 ms bins."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Tuple

Bar = Dict[str, float]


def read_jsonl(path: Path, limit: int | None = None) -> Iterator[Dict[str, object]]:
    count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            yield data
            count += 1
            if limit is not None and count >= limit:
                break


def floor_second(ts_ms: int) -> int:
    return ts_ms - (ts_ms % 1000)


def build_expected_bars(records: Iterable[Dict[str, object]]) -> Dict[str, Bar]:
    per_bar: Dict[Tuple[str, int], Dict[str, float]] = {}
    for record in records:
        symbol = str(record.get("symbol") or "UNKNOWN")
        start_ms = int(record.get("start_ts_ms", 0))
        bar_start = floor_second(start_ms)
        key = (symbol, bar_start)
        open_px = float(record.get("open", record.get("price", 0.0)))
        high_px = float(record.get("high", open_px))
        low_px = float(record.get("low", open_px))
        close_px = float(record.get("close", open_px))
        volume = float(record.get("volume", record.get("sz", 0.0)))
        notional = float(record.get("notional", close_px * volume))
        lateness = float(record.get("max_lateness_ms", 0.0))
        if key not in per_bar:
            per_bar[key] = {
                "open": open_px,
                "high": high_px,
                "low": low_px,
                "close": close_px,
                "volume": volume,
                "notional": notional,
                "bin_count": 1.0,
                "max_lateness_ms": lateness,
            }
        else:
            state = per_bar[key]
            state["high"] = max(state["high"], high_px)
            state["low"] = min(state["low"], low_px)
            state["close"] = close_px
            state["volume"] += volume
            state["notional"] += notional
            state["bin_count"] += 1.0
            state["max_lateness_ms"] = max(state["max_lateness_ms"], lateness)
    expected: Dict[str, Bar] = {}
    for (symbol, bar_start), state in per_bar.items():
        bar_id = f"{symbol}-{bar_start:013d}"
        volume = round(state["volume"], 6)
        notional = round(state["notional"], 6)
        vwap = round(notional / volume, 6) if volume else state["close"]
        expected[bar_id] = {
            "symbol": symbol,
            "start_ts_ms": float(bar_start),
            "end_ts_ms": float(bar_start + 1000),
            "open": state["open"],
            "high": state["high"],
            "low": state["low"],
            "close": state["close"],
            "volume": volume,
            "notional": notional,
            "vwap": vwap,
            "bin_count": state["bin_count"],
            "max_lateness_ms": state["max_lateness_ms"],
        }
    return expected


def load_actual_bars(path: Path, limit: int | None = None) -> Dict[str, Dict[str, object]]:
    bars: Dict[str, Dict[str, object]] = {}
    for record in read_jsonl(path, limit):
        bar_id = str(record.get("bar_id"))
        if not bar_id:
            continue
        bars[bar_id] = record
    return bars


def compare_bars(
    expected: Dict[str, Bar],
    actual: Dict[str, Dict[str, object]],
    tolerance: float,
) -> Tuple[List[str], List[str], List[Tuple[str, str, float, float]]]:
    missing: List[str] = []
    extra = [bar_id for bar_id in actual.keys() if bar_id not in expected]
    mismatches: List[Tuple[str, str, float, float]] = []
    keys_to_check = ["open", "high", "low", "close", "volume", "notional", "vwap", "bin_count"]
    for bar_id, exp in expected.items():
        actual_bar = actual.get(bar_id)
        if actual_bar is None:
            missing.append(bar_id)
            continue
        for key in keys_to_check:
            actual_value = float(actual_bar.get(key, 0.0))
            expected_value = float(exp.get(key, 0.0))
            if not math.isclose(actual_value, expected_value, rel_tol=0.0, abs_tol=tolerance):
                mismatches.append((bar_id, key, expected_value, actual_value))
    return missing, extra, mismatches


def compute_latency_stats(bars: Dict[str, Dict[str, object]]) -> Dict[str, float]:
    latencies: List[float] = []
    for bar in bars.values():
        end_ts = float(bar.get("end_ts_ms", 0.0))
        ingest_ts = float(bar.get("ingest_ts_ms", end_ts))
        latencies.append(max(ingest_ts - end_ts, 0.0))
    if not latencies:
        return {"count": 0.0, "p95": 0.0, "max": 0.0}
    sorted_vals = sorted(latencies)
    p95_index = min(len(sorted_vals) - 1, int(len(sorted_vals) * 0.95))
    return {
        "count": float(len(latencies)),
        "p95": sorted_vals[p95_index],
        "max": max(sorted_vals),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bins", required=True, help="Path to replay bin JSONL (e.g. logs/replay.jsonl)")
    parser.add_argument("--bars", required=True, help="Path to aggregator JSONL (e.g. logs/aggregate.jsonl)")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit on number of records to compare")
    parser.add_argument(
        "--tolerance",
        type=float,
        default=1e-6,
        help="Absolute tolerance for numeric comparisons (default: 1e-6)",
    )
    args = parser.parse_args()

    bins_path = Path(args.bins)
    bars_path = Path(args.bars)
    if not bins_path.exists():
        parser.error(f"bin file not found: {bins_path}")
    if not bars_path.exists():
        parser.error(f"bar file not found: {bars_path}")

    expected = build_expected_bars(read_jsonl(bins_path, args.limit))
    actual = load_actual_bars(bars_path, args.limit)
    missing, extra, mismatches = compare_bars(expected, actual, args.tolerance)
    latency_stats = compute_latency_stats(actual)

    print("--- Validation Summary ---")
    print(f"Expected bars: {len(expected)}")
    print(f"Actual bars:   {len(actual)}")
    print(f"Missing bars:  {len(missing)}")
    print(f"Extra bars:    {len(extra)}")
    print(f"Field diffs:   {len(mismatches)} (tolerance={args.tolerance})")
    print(f"Latency p95:   {latency_stats['p95']:.2f} ms | max: {latency_stats['max']:.2f} ms")

    if missing:
        print("\nMissing examples:")
        for bar_id in missing[:5]:
            print(f"  - {bar_id}")
    if extra:
        print("\nUnexpected examples:")
        for bar_id in extra[:5]:
            print(f"  - {bar_id}")
    if mismatches:
        print("\nSample mismatches:")
        for bar_id, field, expected_value, actual_value in mismatches[:5]:
            print(f"  - {bar_id}::{field}: expected {expected_value} got {actual_value}")

    success = not missing and not extra and not mismatches
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())



