#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path


PREFERRED_COLUMNS = (
    "stusps",
    "state",
    "state_abbr",
    "state_code",
    "abbr",
    "postal",
)


def _pick_column(fieldnames: list[str]) -> str | None:
    normalized = {str(name).strip().lower(): str(name) for name in (fieldnames or []) if str(name).strip()}
    for key in PREFERRED_COLUMNS:
        if key in normalized:
            return normalized[key]
    if fieldnames:
        first = str(fieldnames[0]).strip()
        if first:
            return first
    return None


def _read_states(csv_path: Path) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        col = _pick_column(list(reader.fieldnames or []))
        if not col:
            raise ValueError("states csv has no usable columns")
        for row in reader:
            raw = str((row or {}).get(col) or "").strip()
            if not raw:
                continue
            token = raw.upper()
            if token in seen:
                continue
            seen.add(token)
            out.append(token)
    if not out:
        raise ValueError("no state values found in csv")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Convert states.of.interest.csv to a comma-separated value list.")
    ap.add_argument("--csv", required=True, help="Path to states.of.interest.csv")
    args = ap.parse_args()

    csv_path = Path(args.csv).expanduser().resolve()
    if not csv_path.exists():
        raise FileNotFoundError(f"csv not found: {csv_path}")

    values = _read_states(csv_path)
    text = ",".join(values)
    # No trailing newline keeps downstream token interpolation stable.
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
