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
    for key in ["county_name", "raster_path"]:
        text = str(row.get(key) or "").strip()
        match = TILE_RE.search(text)
        if match:
            return match.group(1).lower()
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
        county_id = str(row.get("county_id") or "").strip()
        year = str(row.get("day") or "").strip()
        unscaled_yield = str(row.get("unscaled_yield_mean") or "").strip()
        pixel_count = str(row.get("unscaled_yield_count") or "").strip()
        if not county_id or not year or not unscaled_yield:
            continue
        tile_field_id = county_id
        if "_" not in tile_field_id:
            tile_coord = _derive_tile_coord(row)
            if tile_coord:
                tile_field_id = f"{tile_coord}_{county_id}"
        tile_coord, field_id = _split_tile_field_id(tile_field_id)
        normalized_rows.append(
            {
                "tile_coord": tile_coord,
                "field_ID": field_id,
                "tile_field_ID": tile_field_id,
                "year": year,
                "unscaled_yield": unscaled_yield,
                "pixel_count": pixel_count,
                "raster_path": str(row.get("raster_path") or "").strip(),
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
