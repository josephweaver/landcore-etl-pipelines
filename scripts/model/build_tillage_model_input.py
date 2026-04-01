#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    return str(value).strip()


def _key_tile_year(row: dict[str, Any]) -> tuple[str, str]:
    return (_to_text(row.get("tile_field_ID")), _to_text(row.get("year")))


def _read_keyed_csv(
    path: Path,
    *,
    key_fields: list[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"input csv not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            raise RuntimeError(f"input csv has no header: {path}")
        fieldnames = [str(x) for x in rdr.fieldnames]
        missing = [c for c in key_fields if c not in fieldnames]
        if missing:
            raise ValueError(f"input csv missing required key fields {missing}: {path}")
        rows = [{str(k): v for k, v in (row or {}).items()} for row in rdr]
    return rows, fieldnames


def _index_rows(
    rows: list[dict[str, Any]],
    *,
    key_fields: list[str],
    label: str,
) -> dict[tuple[str, ...], dict[str, Any]]:
    indexed: dict[tuple[str, ...], dict[str, Any]] = {}
    duplicates: list[tuple[str, ...]] = []
    for row in rows:
        key = tuple(_to_text(row.get(field)) for field in key_fields)
        if any(not part for part in key):
            continue
        if key in indexed:
            duplicates.append(key)
            continue
        indexed[key] = row
    if duplicates:
        preview = ", ".join(str(x) for x in duplicates[:5])
        raise RuntimeError(f"{label} contains duplicate keys for {key_fields}: {preview}")
    return indexed


def _to_annual_tillage(value: str) -> str:
    raw = _to_text(value)
    if raw == "":
        return ""
    try:
        return str(float(raw))
    except ValueError as exc:
        raise ValueError(f"dominant_tillage is not numeric 0/1: {raw}") from exc


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a tillage MODEL_IN table from staged field-year covariates.")
    ap.add_argument("--corn-csv", required=True)
    ap.add_argument("--tillage-csv", required=True)
    ap.add_argument("--vpdmax-csv", required=True)
    ap.add_argument("--nccpi-csv", required=True)
    ap.add_argument("--field-fips-csv", required=True)
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    corn_csv = Path(str(args.corn_csv)).expanduser().resolve()
    tillage_csv = Path(str(args.tillage_csv)).expanduser().resolve()
    vpdmax_csv = Path(str(args.vpdmax_csv)).expanduser().resolve()
    nccpi_csv = Path(str(args.nccpi_csv)).expanduser().resolve()
    field_fips_csv = Path(str(args.field_fips_csv)).expanduser().resolve()
    output_csv = Path(str(args.output_csv)).expanduser().resolve()
    summary_json = Path(str(args.summary_json)).expanduser().resolve()
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    corn_rows, _ = _read_keyed_csv(corn_csv, key_fields=["tile_field_ID", "year"])
    tillage_rows, _ = _read_keyed_csv(tillage_csv, key_fields=["tile_field_ID", "year"])
    vpd_rows, _ = _read_keyed_csv(vpdmax_csv, key_fields=["tile_field_ID", "year"])
    nccpi_rows, _ = _read_keyed_csv(nccpi_csv, key_fields=["tile_field_id"])
    fips_rows, _ = _read_keyed_csv(field_fips_csv, key_fields=["tile_field_ID"])

    corn_by_key = _index_rows(corn_rows, key_fields=["tile_field_ID", "year"], label="corn")
    tillage_by_key = _index_rows(tillage_rows, key_fields=["tile_field_ID", "year"], label="tillage")
    vpd_by_key = _index_rows(vpd_rows, key_fields=["tile_field_ID", "year"], label="vpdmax")
    nccpi_by_key = _index_rows(nccpi_rows, key_fields=["tile_field_id"], label="nccpi")
    fips_by_key = _index_rows(fips_rows, key_fields=["tile_field_ID"], label="field_fips")

    output_rows: list[dict[str, Any]] = []
    missing_tillage = 0
    missing_vpd = 0
    missing_nccpi = 0
    missing_fips = 0
    missing_required_value = 0

    for tile_field_id, year in sorted(corn_by_key.keys(), key=lambda x: (x[0], int(x[1]))):
        corn = corn_by_key[(tile_field_id, year)]
        tillage = tillage_by_key.get((tile_field_id, year))
        if tillage is None:
            missing_tillage += 1
            continue
        vpd = vpd_by_key.get((tile_field_id, year))
        if vpd is None:
            missing_vpd += 1
            continue
        nccpi = nccpi_by_key.get((tile_field_id,))
        if nccpi is None:
            missing_nccpi += 1
            continue
        fips = fips_by_key.get((tile_field_id,))
        if fips is None:
            missing_fips += 1
            continue

        annual_tillage = _to_annual_tillage(_to_text(tillage.get("dominant_tillage")))
        row = {
            "tile_coord": _to_text(corn.get("tile_coord") or tillage.get("tile_coord") or fips.get("tile_coord")),
            "field_ID": _to_text(corn.get("field_ID") or tillage.get("field_ID") or fips.get("yanroy_field_id")),
            "tile_field_ID": tile_field_id,
            "FIPS": _to_text(fips.get("FIPS")),
            "state": _to_text(fips.get("STATEFP")),
            "county": _to_text(fips.get("county")),
            "year": year,
            "unscaled_yield": _to_text(corn.get("unscaled_yield")),
            "annual_tillage": annual_tillage,
            "dominant_tillage": _to_text(tillage.get("dominant_tillage")),
            "NCCPI": _to_text(nccpi.get("nccpi3corn")),
            "nccpi3corn": _to_text(nccpi.get("nccpi3corn")),
            "nccpi3all": _to_text(nccpi.get("nccpi3all")),
            "nccpi3soy": _to_text(nccpi.get("nccpi3soy")),
            "vpdmax_7": _to_text(vpd.get("vpdmax_7")),
            "vpdmax_8": _to_text(vpd.get("vpdmax_8")),
            "tillage_0_prop": _to_text(tillage.get("tillage_0_prop")),
            "tillage_1_prop": _to_text(tillage.get("tillage_1_prop")),
            "tillage_na_prop": _to_text(tillage.get("tillage_na_prop")),
        }
        required = ["FIPS", "year", "unscaled_yield", "annual_tillage", "NCCPI", "vpdmax_7"]
        if any(not _to_text(row.get(col)) for col in required):
            missing_required_value += 1
            continue
        output_rows.append(row)

    fieldnames = [
        "tile_coord",
        "field_ID",
        "tile_field_ID",
        "FIPS",
        "state",
        "county",
        "year",
        "unscaled_yield",
        "annual_tillage",
        "dominant_tillage",
        "NCCPI",
        "nccpi3corn",
        "nccpi3all",
        "nccpi3soy",
        "vpdmax_7",
        "vpdmax_8",
        "tillage_0_prop",
        "tillage_1_prop",
        "tillage_na_prop",
    ]
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    duplicates = len(output_rows) - len({(r["tile_field_ID"], r["year"]) for r in output_rows})
    summary = {
        "corn_csv": corn_csv.as_posix(),
        "tillage_csv": tillage_csv.as_posix(),
        "vpdmax_csv": vpdmax_csv.as_posix(),
        "nccpi_csv": nccpi_csv.as_posix(),
        "field_fips_csv": field_fips_csv.as_posix(),
        "output_csv": output_csv.as_posix(),
        "corn_row_count": len(corn_rows),
        "tillage_row_count": len(tillage_rows),
        "vpd_row_count": len(vpd_rows),
        "nccpi_row_count": len(nccpi_rows),
        "field_fips_row_count": len(fips_rows),
        "output_row_count": len(output_rows),
        "duplicate_tile_field_year_rows": duplicates,
        "missing_tillage_rows": missing_tillage,
        "missing_vpd_rows": missing_vpd,
        "missing_nccpi_rows": missing_nccpi,
        "missing_fips_rows": missing_fips,
        "missing_required_value_rows": missing_required_value,
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if args.verbose:
        print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
