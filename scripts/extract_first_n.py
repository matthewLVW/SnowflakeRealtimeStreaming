#!/usr/bin/env python3
"""Write the first N lines from a text file to stdout or another file."""
import argparse
from pathlib import Path
from typing import Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract the first N lines from a text file."
    )
    parser.add_argument("input", type=Path, help="Path to the source file")
    parser.add_argument(
        "--lines",
        type=int,
        default=20000,
        help="Number of lines to copy (default: 20000)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional output file path. Defaults to stdout if omitted.",
    )
    return parser.parse_args()


def extract_lines(source: Path, output: Optional[Path], lines: int) -> None:
    if lines <= 0:
        raise ValueError("The number of lines must be positive")

    count = 0
    if output is None:
        with source.open("r", encoding="utf-8") as handle:
            for line in handle:
                print(line.rstrip("\n"))
                count += 1
                if count >= lines:
                    break
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        with source.open("r", encoding="utf-8") as src, output.open(
            "w", encoding="utf-8", newline=""
        ) as dest:
            for line in src:
                dest.write(line)
                count += 1
                if count >= lines:
                    break


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise FileNotFoundError(f"Input file not found: {args.input}")
    extract_lines(args.input, args.output, args.lines)


if __name__ == "__main__":
    main()
