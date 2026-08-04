#!/usr/bin/env python3
"""Merge per-year/tile field-crop outputs into delivery CSVs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from field_crop_common import read_csv_rows, safe_int, sha256_file, write_csv_rows


COUNTS_FIELDS = ["field_id", "crop_id", "year", "tile", "count"]
SUMMARY_FIELDS = [
    "field_id",
    "year",
    "tile",
    "crop_id",
    "pixel_count",
    "total_field_pixels",
    "share",
    "is_dominant",
    "dominant_crop_id",
    "dominant_crop_share",
]


def load_work_units(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        payload = payload.get("work_units")
    if not isinstance(payload, list):
        raise ValueError("work units must be a list or an object with work_units")
    units: list[dict[str, Any]] = []
    for index, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"work unit {index} must be an object")
        units.append(item)
    return units


def require_path(unit: dict[str, Any], key: str, index: int) -> Path:
    value = str(unit.get(key, "")).strip()
    if not value:
        raise ValueError(f"work unit {index} missing {key}")
    path = Path(value)
    if not path.exists():
        raise FileNotFoundError(f"work unit {index} {key} does not exist: {path}")
    return path


def merge_counts(units: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[int, int, int, str]] = set()
    for index, unit in enumerate(units, start=1):
        year = safe_int(unit.get("year"), f"work unit {index} year")
        tile = str(unit.get("tile", "")).strip()
        if not tile:
            raise ValueError(f"work unit {index} missing tile")
        for row_index, row in enumerate(read_csv_rows(require_path(unit, "counts_csv", index)), start=1):
            field_id = safe_int(row.get("field_id"), f"unit {index} counts row {row_index} field_id")
            crop_id = safe_int(row.get("crop_id"), f"unit {index} counts row {row_index} crop_id")
            count = safe_int(row.get("count"), f"unit {index} counts row {row_index} count")
            key = (field_id, crop_id, year, tile)
            if key in seen:
                raise ValueError(f"duplicate merged counts key: {key}")
            seen.add(key)
            rows.append(
                {
                    "field_id": str(field_id),
                    "crop_id": str(crop_id),
                    "year": str(year),
                    "tile": tile,
                    "count": str(count),
                }
            )
    return sorted(rows, key=lambda row: (int(row["year"]), row["tile"], int(row["field_id"]), int(row["crop_id"])))


def merge_summary(units: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[int, int, int, str]] = set()
    for index, unit in enumerate(units, start=1):
        year = safe_int(unit.get("year"), f"work unit {index} year")
        tile = str(unit.get("tile", "")).strip()
        if not tile:
            raise ValueError(f"work unit {index} missing tile")
        for row_index, row in enumerate(read_csv_rows(require_path(unit, "summary_csv", index)), start=1):
            field_id = safe_int(row.get("field_id"), f"unit {index} summary row {row_index} field_id")
            crop_id = safe_int(row.get("crop_id"), f"unit {index} summary row {row_index} crop_id")
            row_year = safe_int(row.get("year"), f"unit {index} summary row {row_index} year")
            if row_year != year:
                raise ValueError(f"unit {index} summary row {row_index} year {row_year} != {year}")
            key = (field_id, crop_id, year, tile)
            if key in seen:
                raise ValueError(f"duplicate merged summary key: {key}")
            seen.add(key)
            rows.append({field: str(row.get(field, "")).strip() for field in SUMMARY_FIELDS if field != "tile"})
            rows[-1]["tile"] = tile
    return sorted(rows, key=lambda row: (int(row["year"]), row["tile"], int(row["field_id"]), int(row["crop_id"])))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-units-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--counts-output", default="field_crop_year_counts_all.csv")
    parser.add_argument("--summary-output", default="field_crop_year_summary_all.csv")
    parser.add_argument("--metadata-output", default="merge_metadata.json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    units = load_work_units(Path(args.work_units_json))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    counts_path = output_dir / args.counts_output
    summary_path = output_dir / args.summary_output
    metadata_path = output_dir / args.metadata_output

    counts_rows = merge_counts(units)
    summary_rows = merge_summary(units)
    write_csv_rows(counts_path, COUNTS_FIELDS, counts_rows)
    write_csv_rows(summary_path, SUMMARY_FIELDS, summary_rows)

    metadata = {
        "schema": "landcore/field-crop-year-merge/v1",
        "work_unit_count": len(units),
        "counts_row_count": len(counts_rows),
        "summary_row_count": len(summary_rows),
        "counts_csv": args.counts_output,
        "summary_csv": args.summary_output,
        "counts_sha256": sha256_file(counts_path),
        "summary_sha256": sha256_file(summary_path),
    }
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
