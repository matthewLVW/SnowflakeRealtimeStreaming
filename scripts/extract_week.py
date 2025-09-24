#!/usr/bin/env python3
"""Extract the first full Monday-Friday trading week from a tick data file."""
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional, Sequence, Set


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Write all rows belonging to the first complete Monday-Friday "
            "trading week in the input file."
        )
    )
    parser.add_argument(
        "input",
        type=Path,
        help="Path to the source tick file (CSV with date as first column).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "Optional output file path. If omitted, rows are printed to stdout."
        ),
    )
    return parser.parse_args()


def collect_unique_dates(path: Path) -> Set[datetime.date]:
    dates: Set[datetime.date] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            date_token = line.split(",", 1)[0]
            try:
                dt = datetime.strptime(date_token, "%m/%d/%Y").date()
            except ValueError as exc:
                raise ValueError(f"Could not parse date '{date_token}'") from exc
            dates.add(dt)
    return dates


def find_first_complete_week(dates: Set[datetime.date]) -> Sequence[datetime.date]:
    ordered = sorted(dates)
    for day in ordered:
        if day.weekday() != 0:  # 0 = Monday
            continue
        week = [day + timedelta(days=offset) for offset in range(5)]
        if all(member in dates for member in week):
            return week
    raise ValueError("No complete Monday-Friday week found in dataset")


def iter_week_rows(source: Path, week_dates: Sequence[datetime.date]) -> Iterable[str]:
    date_tokens = {dt.strftime("%m/%d/%Y") for dt in week_dates}
    with source.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            date_token = line.split(",", 1)[0]
            if date_token in date_tokens:
                yield line


def emit_week_rows(
    source: Path, week_dates: Sequence[datetime.date], output: Optional[Path]
) -> None:
    rows = iter_week_rows(source, week_dates)
    if output is None:
        for row in rows:
            print(row.rstrip("\n"))
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", encoding="utf-8", newline="") as handle:
            handle.writelines(rows)


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise FileNotFoundError(f"Input file not found: {args.input}")
    dates = collect_unique_dates(args.input)
    week = find_first_complete_week(dates)
    emit_week_rows(args.input, week, args.output)


if __name__ == "__main__":
    main()
