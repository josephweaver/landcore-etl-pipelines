#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
import re


TARGET_MONTHS = (7, 8)
TILE_RE = re.compile(r"(?i)(h\d{2}v\d{2})")


def _resolve(path_text: str) -> Path:
    return Path(path_text).expanduser().resolve()


def _split_tile_field_id(value: str) -> tuple[str, str]:
    text = str(value or "").strip()
    if "_" not in text:
        return "", ""
    left, right = text.split("_", 1)
    return left, right


def _tile_from_source_name(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = TILE_RE.search(text)
    if not match:
        return ""
    return str(match.group(1) or "").lower()


def _parse_year_month(value: str) -> tuple[str, str]:
    text = str(value or "").strip()
    if len(text) != 6 or not text.isdigit():
        return "", ""
    return text[:4], str(int(text[4:]))


def _pick_value(row: dict[str, str]) -> str:
    for column in ("vpdmax_mean", "vpdmax_avg", "vpdmax_max"):
        value = str(row.get(column) or "").strip()
        if value:
            return value
    return ""


def _field_id_from_row(row: dict[str, str]) -> str:
    for column in ("field_id", "county_id"):
        value = str(row.get(column) or "").strip()
        if value:
            return value
    return ""


def main() -> int:
    ap = argparse.ArgumentParser(description="Normalize monthly PRISM field aggregates into one row per tile field and year.")
    ap.add_argument("--input-csv", required=True)
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--summary-json", required=True)
    args = ap.parse_args()

    input_csv = _resolve(args.input_csv)
    output_csv = _resolve(args.output_csv)
    summary_json = _resolve(args.summary_json)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    with input_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError(f"input csv has no header: {input_csv}")
        rows = list(reader)

    grouped: dict[tuple[str, str], dict[str, str]] = {}
    duplicate_month_rows = 0
    skipped_rows = 0

    for row in rows:
        field_id = _field_id_from_row(row)
        tile_coord = str(row.get("tile_coord") or "").strip().lower()
        tile_field_id = str(row.get("tile_field_ID") or row.get("tile_field_id") or "").strip()
        if not tile_field_id and tile_coord and field_id:
            tile_field_id = f"{tile_coord}_{field_id}"
        if not tile_field_id and field_id and "_" in field_id:
            tile_field_id = field_id
        if not tile_coord and tile_field_id:
            tile_coord = _tile_from_source_name(tile_field_id)
        year, month = _parse_year_month(str(row.get("day") or ""))
        value = _pick_value(row)
        if not tile_field_id or not year or not month or not value:
            skipped_rows += 1
            continue
        if int(month) not in TARGET_MONTHS:
            continue
        key = (tile_field_id, year)
        out = grouped.setdefault(key, {"tile_field_ID": tile_field_id, "year": year})
        month_key = f"vpdmax_{month}"
        if out.get(month_key):
            duplicate_month_rows += 1
        out[month_key] = value

    normalized_rows: list[dict[str, str]] = []
    missing_month_counts = defaultdict(int)
    for (tile_field_id, year) in sorted(grouped.keys(), key=lambda item: (item[0], int(item[1]))):
        row = grouped[(tile_field_id, year)]
        tile_coord, field_id = _split_tile_field_id(tile_field_id)
        out = {
            "tile_coord": tile_coord,
            "field_ID": field_id,
            "tile_field_ID": tile_field_id,
            "year": year,
            "vpdmax_7": str(row.get("vpdmax_7") or ""),
            "vpdmax_8": str(row.get("vpdmax_8") or ""),
        }
        for month in TARGET_MONTHS:
            if not out[f"vpdmax_{month}"]:
                missing_month_counts[f"vpdmax_{month}"] += 1
        normalized_rows.append(out)

    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["tile_coord", "field_ID", "tile_field_ID", "year", "vpdmax_7", "vpdmax_8"],
        )
        writer.writeheader()
        writer.writerows(normalized_rows)

    summary = {
        "input_csv": input_csv.as_posix(),
        "output_csv": output_csv.as_posix(),
        "row_count": len(normalized_rows),
        "duplicate_month_rows": duplicate_month_rows,
        "skipped_rows": skipped_rows,
        "missing_month_counts": dict(missing_month_counts),
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
