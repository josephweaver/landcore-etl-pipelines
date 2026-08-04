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


def _to_float_or_text(value: Any) -> float | str:
    text = _to_text(value)
    if not text:
        return ""
    try:
        return float(text)
    except Exception:
        return text


def _read_valu1_by_mukey(path: Path, *, mukey_field: str, value_fields: list[str]) -> tuple[dict[str, dict[str, Any]], int]:
    if not path.exists():
        raise FileNotFoundError(f"valu1 csv not found: {path}")
    by_mukey: dict[str, dict[str, Any]] = {}
    duplicate_mukey_count = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            raise ValueError("valu1 csv has no header")
        missing = [c for c in [mukey_field, *value_fields] if c not in rdr.fieldnames]
        if missing:
            raise ValueError(f"valu1 csv missing required columns: {missing}")
        for row in rdr:
            mukey = _to_text((row or {}).get(mukey_field))
            if not mukey:
                continue
            values = {field: _to_float_or_text((row or {}).get(field)) for field in value_fields}
            prior = by_mukey.get(mukey)
            if prior is None:
                by_mukey[mukey] = values
                continue
            duplicate_mukey_count += 1
            if prior != values:
                raise ValueError(f"conflicting valu1 rows for mukey={mukey}")
    if not by_mukey:
        raise RuntimeError("no valu1 mukey rows found")
    return by_mukey, duplicate_mukey_count


def main() -> int:
    ap = argparse.ArgumentParser(description="Join tile_field_id MUKEY overlap rows to SSURGO Valu1 NCCPI values.")
    ap.add_argument("--tile-field-mukey-csv", required=True)
    ap.add_argument("--valu1-csv", required=True)
    ap.add_argument("--field-id-field", default="tile_field_id")
    ap.add_argument("--mukey-field", default="mukey")
    ap.add_argument("--weight-field", default="pct_overlap")
    ap.add_argument("--overlap-area-field", default="overlap_area")
    ap.add_argument("--value-fields", default="nccpi3all,nccpi3corn,nccpi3soy")
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    tile_field_mukey_csv = Path(str(args.tile_field_mukey_csv)).expanduser().resolve()
    valu1_csv = Path(str(args.valu1_csv)).expanduser().resolve()
    output_csv = Path(str(args.output_csv)).expanduser().resolve()
    summary_json = Path(str(args.summary_json)).expanduser().resolve()
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    field_id_field = _to_text(args.field_id_field) or "tile_field_id"
    mukey_field = _to_text(args.mukey_field) or "mukey"
    weight_field = _to_text(args.weight_field) or "pct_overlap"
    overlap_area_field = _to_text(args.overlap_area_field) or "overlap_area"
    value_fields = [x.strip() for x in str(args.value_fields or "").split(",") if x.strip()]
    if not value_fields:
        raise ValueError("value_fields must not be empty")

    valu1_by_mukey, duplicate_mukey_count = _read_valu1_by_mukey(
        valu1_csv,
        mukey_field=mukey_field,
        value_fields=value_fields,
    )

    joined_rows = 0
    missing_mukey_rows = 0
    input_rows = 0
    field_ids_seen: set[str] = set()
    mukeys_joined: set[str] = set()

    with tile_field_mukey_csv.open("r", encoding="utf-8-sig", newline="") as src, output_csv.open(
        "w", encoding="utf-8", newline=""
    ) as dst:
        rdr = csv.DictReader(src)
        if not rdr.fieldnames:
            raise ValueError("tile_field_mukey csv has no header")
        missing = [c for c in [field_id_field, mukey_field, weight_field] if c not in rdr.fieldnames]
        if missing:
            raise ValueError(f"tile_field_mukey csv missing required columns: {missing}")

        output_fields = [field_id_field, mukey_field, weight_field]
        if overlap_area_field in rdr.fieldnames:
            output_fields.append(overlap_area_field)
        output_fields.extend(value_fields)
        w = csv.DictWriter(dst, fieldnames=output_fields)
        w.writeheader()

        for row in rdr:
            input_rows += 1
            field_id = _to_text((row or {}).get(field_id_field))
            mukey = _to_text((row or {}).get(mukey_field))
            if not field_id or not mukey:
                continue
            valu1 = valu1_by_mukey.get(mukey)
            if valu1 is None:
                missing_mukey_rows += 1
                continue
            out: dict[str, Any] = {
                field_id_field: field_id,
                mukey_field: mukey,
                weight_field: _to_text((row or {}).get(weight_field)),
            }
            if overlap_area_field in output_fields:
                out[overlap_area_field] = _to_text((row or {}).get(overlap_area_field))
            for value_field in value_fields:
                out[value_field] = valu1.get(value_field, "")
            w.writerow(out)
            joined_rows += 1
            field_ids_seen.add(field_id)
            mukeys_joined.add(mukey)

    summary = {
        "tile_field_mukey_csv": tile_field_mukey_csv.as_posix(),
        "valu1_csv": valu1_csv.as_posix(),
        "output_csv": output_csv.as_posix(),
        "field_id_field": field_id_field,
        "mukey_field": mukey_field,
        "weight_field": weight_field,
        "value_fields": value_fields,
        "tile_field_mukey_row_count": input_rows,
        "valu1_mukey_count": len(valu1_by_mukey),
        "valu1_duplicate_mukey_count": duplicate_mukey_count,
        "joined_row_count": joined_rows,
        "joined_field_count": len(field_ids_seen),
        "joined_mukey_count": len(mukeys_joined),
        "missing_mukey_rows": missing_mukey_rows,
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if args.verbose:
        print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
