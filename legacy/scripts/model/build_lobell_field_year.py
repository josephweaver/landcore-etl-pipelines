#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
from pathlib import Path


def _resolve(path_text: str) -> Path:
    return Path(path_text).expanduser().resolve()


def _iter_files(pattern_text: str) -> list[Path]:
    matches: list[Path] = []
    seen: set[str] = set()
    for token in str(pattern_text or "").replace(";", ",").split(","):
        pattern = token.strip()
        if not pattern:
            continue
        for raw in sorted(glob.glob(pattern, recursive=True)):
            path = Path(raw).resolve()
            key = path.as_posix().lower()
            if key in seen or not path.is_file():
                continue
            seen.add(key)
            matches.append(path)
    return matches


def _parse_pct(value: str) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _nonblank(value: str) -> bool:
    return bool(str(value or "").strip())


def _tile_field_id(tile_coord: str, field_id: str) -> str:
    return f"{str(tile_coord or '').strip().lower()}_{str(field_id or '').strip()}"


def _read_filtered_rows(files: list[Path], *, value_column: str) -> tuple[dict[tuple[str, str, str], dict[str, str]], dict[str, int]]:
    rows_by_key: dict[tuple[str, str, str], dict[str, str]] = {}
    stats = {"files": len(files), "rows_read": 0, "rows_kept": 0, "rows_dropped": 0}
    for path in files:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                stats["rows_read"] += 1
                tile_coord = str(row.get("tile_coord") or "").strip()
                field_id = str(row.get("field_ID") or row.get("field_id") or "").strip()
                year = str(row.get("year") or "").strip()
                value = str(row.get(value_column) or "").strip()
                pct_na = _parse_pct(str(row.get("pct_na_all") or ""))
                if not (_nonblank(tile_coord) and _nonblank(field_id) and _nonblank(year) and _nonblank(value)):
                    stats["rows_dropped"] += 1
                    continue
                if pct_na is not None and pct_na > 0.9:
                    stats["rows_dropped"] += 1
                    continue
                key = (tile_coord, field_id, year)
                rows_by_key[key] = dict(row)
                stats["rows_kept"] += 1
    return rows_by_key, stats


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build a canonical Lobell field-year table from historical corn/tillage/soy tile CSV outputs."
    )
    ap.add_argument("--corn-glob", required=True)
    ap.add_argument("--tillage-glob", required=True)
    ap.add_argument("--soy-glob", default="")
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--summary-json", required=True)
    args = ap.parse_args()

    corn_files = _iter_files(args.corn_glob)
    tillage_files = _iter_files(args.tillage_glob)
    soy_files = _iter_files(args.soy_glob)
    if not corn_files:
      raise RuntimeError("no corn tile csv files matched --corn-glob")
    if not tillage_files:
      raise RuntimeError("no tillage tile csv files matched --tillage-glob")

    corn_rows, corn_stats = _read_filtered_rows(corn_files, value_column="corn_yield_mean")
    tillage_rows, tillage_stats = _read_filtered_rows(tillage_files, value_column="dominant_tillage")
    soy_rows, soy_stats = _read_filtered_rows(soy_files, value_column="soy_yield_mean") if soy_files else ({}, {"files": 0, "rows_read": 0, "rows_kept": 0, "rows_dropped": 0})

    merged_rows: list[dict[str, str]] = []
    join_keys = sorted(set(corn_rows.keys()) | set(tillage_rows.keys()) | set(soy_rows.keys()))
    for key in join_keys:
        tile_coord, field_id, year = key
        corn = corn_rows.get(key, {})
        tillage = tillage_rows.get(key, {})
        soy = soy_rows.get(key, {})
        merged_rows.append(
            {
                "tile_coord": tile_coord,
                "field_ID": field_id,
                "tile_field_ID": _tile_field_id(tile_coord, field_id),
                "year": year,
                "unscaled_yield": str(corn.get("corn_yield_mean") or ""),
                "dominant_tillage": str(tillage.get("dominant_tillage") or ""),
                "soy_yield_mean": str(soy.get("soy_yield_mean") or ""),
            }
        )

    output_csv = _resolve(args.output_csv)
    summary_json = _resolve(args.summary_json)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "tile_coord",
                "field_ID",
                "tile_field_ID",
                "year",
                "unscaled_yield",
                "dominant_tillage",
                "soy_yield_mean",
            ],
        )
        writer.writeheader()
        writer.writerows(merged_rows)

    summary = {
        "corn_files": [p.as_posix() for p in corn_files],
        "tillage_files": [p.as_posix() for p in tillage_files],
        "soy_files": [p.as_posix() for p in soy_files],
        "corn_stats": corn_stats,
        "tillage_stats": tillage_stats,
        "soy_stats": soy_stats,
        "output_csv": output_csv.as_posix(),
        "row_count": len(merged_rows),
        "rows_with_corn": sum(1 for row in merged_rows if _nonblank(row["unscaled_yield"])),
        "rows_with_tillage": sum(1 for row in merged_rows if _nonblank(row["dominant_tillage"])),
        "rows_with_soy": sum(1 for row in merged_rows if _nonblank(row["soy_yield_mean"])),
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
