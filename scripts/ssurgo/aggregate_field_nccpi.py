#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
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
    return str(value)


def _to_float(value: Any) -> float | None:
    text = _to_text(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Aggregate MUKEY-level NCCPI rows to weighted field-level averages.")
    ap.add_argument("--input-csv", required=True, help="Input CSV with tile_field_id, mukey, pct_overlap, and NCCPI columns")
    ap.add_argument("--field-id-field", default="tile_field_id")
    ap.add_argument("--weight-field", default="pct_overlap")
    ap.add_argument("--value-fields", default="nccpi3all,nccpi3corn,nccpi3soy")
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    input_csv = Path(args.input_csv).expanduser().resolve()
    output_csv = Path(args.output_csv).expanduser().resolve()
    summary_json = Path(args.summary_json).expanduser().resolve()
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    value_fields = [x.strip() for x in str(args.value_fields or "").split(",") if x.strip()]
    if not value_fields:
        raise ValueError("value_fields must not be empty")

    field_id_field = str(args.field_id_field)
    weight_field = str(args.weight_field)

    numerators: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    denominators: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    row_count = 0

    with input_csv.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            raise ValueError("input csv has no header")
        missing = [c for c in [field_id_field, weight_field, *value_fields] if c not in rdr.fieldnames]
        if missing:
            raise ValueError(f"input csv missing required columns: {missing}")

        for row in rdr:
            row_count += 1
            field_id = _to_text((row or {}).get(field_id_field)).strip()
            weight = _to_float((row or {}).get(weight_field))
            if not field_id or weight is None or weight <= 0:
                continue
            for value_field in value_fields:
                value = _to_float((row or {}).get(value_field))
                if value is None:
                    continue
                numerators[field_id][value_field] += weight * value
                denominators[field_id][value_field] += weight

    out_rows: list[dict[str, Any]] = []
    for field_id in sorted(set(numerators.keys()) | set(denominators.keys())):
        out: dict[str, Any] = {field_id_field: field_id}
        for value_field in value_fields:
            denom = denominators[field_id].get(value_field, 0.0)
            out[value_field] = (numerators[field_id][value_field] / denom) if denom > 0 else ""
        out_rows.append(out)

    with output_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[field_id_field, *value_fields])
        w.writeheader()
        w.writerows(out_rows)

    summary = {
        "input_csv": input_csv.as_posix(),
        "output_csv": output_csv.as_posix(),
        "field_id_field": field_id_field,
        "weight_field": weight_field,
        "value_fields": value_fields,
        "input_row_count": row_count,
        "output_row_count": len(out_rows),
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if args.verbose:
        print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
