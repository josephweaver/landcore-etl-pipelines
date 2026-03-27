#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import re


TILE_RE = re.compile(r"(?i)(h\d{2}v\d{2})")


def _resolve(path_text: str) -> Path:
    return Path(path_text).expanduser().resolve()


def _split_tile_field_id(value: str) -> tuple[str, str]:
    text = str(value or "").strip()
    if "_" not in text:
        return "", ""
    left, right = text.split("_", 1)
    return left, right


def _derive_tile_coord(row: dict[str, str]) -> str:
    for key in ["polygon_name", "county_name", "raster_path"]:
        text = str(row.get(key) or "").strip()
        match = TILE_RE.search(text)
        if match:
            return match.group(1).lower()
    return ""


def _pick(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def main() -> int:
    ap = argparse.ArgumentParser(description="Normalize per-year raster aggregate outputs into Lobell corn field-year rows.")
    ap.add_argument("--input-csv", required=True)
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--summary-json", required=True)
    args = ap.parse_args()

    input_csv = _resolve(args.input_csv)
    output_csv = _resolve(args.output_csv)
    summary_json = _resolve(args.summary_json)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    with input_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError(f"input csv has no header: {input_csv}")
        rows = list(reader)

    normalized_rows: list[dict[str, str]] = []
    for row in rows:
        polygon_id = _pick(row, "tile_field_id", "polygon_id", "county_id")
        year = _pick(row, "day", "year")
        unscaled_yield = _pick(row, "unscaled_yield_mean", "corn_yield_mean", "mean")
        pixel_count = _pick(row, "unscaled_yield_count", "pixel_count", "count")
        if not polygon_id or not year or not unscaled_yield:
            continue
        tile_field_id = polygon_id
        if "_" not in tile_field_id:
            tile_coord = _derive_tile_coord(row)
            if tile_coord:
                tile_field_id = f"{tile_coord}_{polygon_id}"
        tile_coord, field_id = _split_tile_field_id(tile_field_id)
        normalized_rows.append(
            {
                "tile_coord": tile_coord,
                "field_ID": field_id,
                "tile_field_ID": tile_field_id,
                "year": year,
                "unscaled_yield": unscaled_yield,
                "pixel_count": pixel_count,
                "raster_path": _pick(row, "raster_path"),
            }
        )

    normalized_rows.sort(key=lambda r: (r["tile_field_ID"], int(r["year"])))

    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "tile_coord",
                "field_ID",
                "tile_field_ID",
                "year",
                "unscaled_yield",
                "pixel_count",
                "raster_path",
            ],
        )
        writer.writeheader()
        writer.writerows(normalized_rows)

    duplicates = len(normalized_rows) - len({(r["tile_field_ID"], r["year"]) for r in normalized_rows})
    summary = {
        "input_csv": input_csv.as_posix(),
        "output_csv": output_csv.as_posix(),
        "row_count": len(normalized_rows),
        "duplicate_tile_field_year_rows": duplicates,
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
