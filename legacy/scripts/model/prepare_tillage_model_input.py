#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


REQUIRED_COLUMNS = [
    "tile_field_ID",
    "FIPS",
    "year",
    "unscaled_yield",
    "annual_tillage",
    "NCCPI",
    "vpdmax_7",
]

OPTIONAL_COLUMNS = [
    "vpdmax_8",
    "state",
    "county",
    "tile_coord",
    "field_ID",
]

ALIASES = {
    "tile_field_ID": ["tile_field_ID", "tile_field_id"],
    "FIPS": ["FIPS", "fips"],
    "year": ["year", "Year"],
    "unscaled_yield": ["unscaled_yield", "yield", "corn_yield_mean", "mean_corn_yield"],
    "annual_tillage": ["annual_tillage", "dominant_tillage", "tillage"],
    "NCCPI": ["NCCPI", "nccpi3corn", "nccpi3all", "nccpi3soy"],
    "vpdmax_7": ["vpdmax_7", "monthly_vpdmax_7"],
    "vpdmax_8": ["vpdmax_8", "monthly_vpdmax_8"],
    "state": ["state", "STATE"],
    "county": ["county", "COUNTY"],
    "tile_coord": ["tile_coord", "tile_id"],
    "field_ID": ["field_ID", "field_id"],
}


def _resolve(value: str) -> Path:
    return Path(value).expanduser().resolve()


def _pick_source(columns: list[str], target: str) -> str:
    for name in ALIASES.get(target, [target]):
        if name in columns:
            return name
    return ""


def _to_number(text: str, field_name: str, *, allow_blank: bool) -> str:
    raw = str(text or "").strip()
    if not raw:
        if allow_blank:
            return ""
        raise ValueError(f"blank value in required field: {field_name}")
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"invalid numeric value for {field_name}: {raw}") from exc
    if field_name == "year":
        return str(int(value))
    return str(value)


def _build_tile_field_id(row: dict[str, str], columns: list[str]) -> str:
    tile_field_id_source = _pick_source(columns, "tile_field_ID")
    if tile_field_id_source:
        value = str(row.get(tile_field_id_source) or "").strip()
        if value:
            return value

    tile_source = _pick_source(columns, "tile_coord")
    field_source = _pick_source(columns, "field_ID")
    tile_value = str(row.get(tile_source) or "").strip().lower() if tile_source else ""
    field_value = str(row.get(field_source) or "").strip() if field_source else ""
    if tile_value and field_value:
        return f"{tile_value}_{field_value}"
    raise ValueError("could not derive tile_field_ID from input row")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Normalize county model input into a tillage-based neighborhood-fit schema."
    )
    ap.add_argument("--input-csv", required=True)
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--summary-json", required=True)
    args = ap.parse_args()

    input_csv = _resolve(args.input_csv)
    output_csv = _resolve(args.output_csv)
    summary_json = _resolve(args.summary_json)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    if not input_csv.exists():
        raise FileNotFoundError(f"input csv not found: {input_csv}")

    with input_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError(f"input csv has no header: {input_csv}")
        source_columns = [str(x) for x in reader.fieldnames]
        rows = list(reader)

    if not rows:
        raise RuntimeError(f"input csv has no data rows: {input_csv}")

    normalized_rows: list[dict[str, str]] = []
    missing_optional_counts = {name: 0 for name in OPTIONAL_COLUMNS}
    alias_map = {name: _pick_source(source_columns, name) for name in REQUIRED_COLUMNS + OPTIONAL_COLUMNS}

    for row in rows:
        out: dict[str, str] = {}
        out["tile_field_ID"] = _build_tile_field_id(row, source_columns)

        for name in ["FIPS", "year", "unscaled_yield", "annual_tillage", "NCCPI", "vpdmax_7"]:
            source_name = alias_map.get(name) or ""
            if not source_name:
                raise ValueError(f"missing required source column for {name}; columns={source_columns}")
            out[name] = _to_number(row.get(source_name, ""), name, allow_blank=False) if name in {
                "year",
                "unscaled_yield",
                "annual_tillage",
                "NCCPI",
                "vpdmax_7",
            } else str(row.get(source_name) or "").strip()
            if name == "FIPS" and not out[name]:
                raise ValueError("blank value in required field: FIPS")

        source_vpd8 = alias_map.get("vpdmax_8") or ""
        out["vpdmax_8"] = _to_number(row.get(source_vpd8, ""), "vpdmax_8", allow_blank=True) if source_vpd8 else ""
        if not out["vpdmax_8"]:
            missing_optional_counts["vpdmax_8"] += 1

        for name in ["state", "county", "tile_coord", "field_ID"]:
            source_name = alias_map.get(name) or ""
            value = str(row.get(source_name) or "").strip() if source_name else ""
            out[name] = value
            if not value:
                missing_optional_counts[name] += 1

        normalized_rows.append(out)

    duplicates = len(normalized_rows) - len({(r["tile_field_ID"], r["year"]) for r in normalized_rows})
    fieldnames = REQUIRED_COLUMNS + ["vpdmax_8"] + [c for c in OPTIONAL_COLUMNS if c != "vpdmax_8"]

    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(normalized_rows)

    summary = {
        "input_csv": input_csv.as_posix(),
        "output_csv": output_csv.as_posix(),
        "row_count": len(normalized_rows),
        "duplicate_tile_field_year_rows": duplicates,
        "source_columns": source_columns,
        "alias_map": alias_map,
        "missing_optional_counts": missing_optional_counts,
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
