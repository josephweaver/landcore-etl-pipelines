from __future__ import annotations

import argparse
import json
import math
from pathlib import Path


def _normalize_field_id(value):
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:  # noqa: BLE001
            pass
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return int(value)
        return value
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    if text.endswith(".0"):
        try:
            parsed = float(text)
            if parsed.is_integer():
                return int(parsed)
        except Exception:  # noqa: BLE001
            pass
    try:
        parsed_int = int(text)
        return parsed_int
    except Exception:  # noqa: BLE001
        pass
    try:
        parsed_float = float(text)
        if parsed_float.is_integer():
            return int(parsed_float)
        return parsed_float
    except Exception:  # noqa: BLE001
        return text


def normalize_polygon_field_ids(
    input_vector: Path,
    output_vector: Path,
    field_id_field: str = "field_id",
    summary_json: Path | None = None,
    overwrite: bool = False,
    verbose: bool = False,
) -> dict:
    try:
        import geopandas as gpd
        import pandas as pd
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("normalize_polygon_field_ids requires geopandas and pandas") from exc

    if not input_vector.exists():
        raise FileNotFoundError(f"input vector not found: {input_vector}")
    if output_vector.exists() and not overwrite:
        raise FileExistsError(f"output vector exists and overwrite not requested: {output_vector}")

    gdf = gpd.read_file(input_vector)
    if gdf.empty:
        raise RuntimeError(f"input vector has no features: {input_vector}")
    if field_id_field not in gdf.columns:
        raise RuntimeError(f"missing field id field: {field_id_field}")

    raw_values = list(gdf[field_id_field].tolist())
    norm_values = [_normalize_field_id(value) for value in raw_values]

    if any(isinstance(value, str) for value in norm_values if value is not None):
        gdf[field_id_field] = [("" if value is None else str(value)) for value in norm_values]
        output_type = "text"
    else:
        gdf[field_id_field] = pd.array(norm_values, dtype="Int64")
        output_type = "integer"

    output_vector.parent.mkdir(parents=True, exist_ok=True)
    if output_vector.exists() and overwrite:
        output_vector.unlink()
    gdf.to_file(output_vector, driver="GPKG")

    changed_rows = 0
    for before, after in zip(raw_values, norm_values):
        before_text = "" if before is None else str(before).strip()
        after_text = "" if after is None else str(after).strip()
        if before_text != after_text:
            changed_rows += 1

    result = {
        "input_vector": input_vector.resolve().as_posix(),
        "output_vector": output_vector.resolve().as_posix(),
        "field_id_field": field_id_field,
        "row_count": int(len(gdf)),
        "changed_rows": int(changed_rows),
        "output_type": output_type,
    }
    if summary_json is not None:
        summary_json.parent.mkdir(parents=True, exist_ok=True)
        summary_json.write_text(json.dumps(result, indent=2), encoding="utf-8")
    if verbose:
        print(json.dumps(result, indent=2))
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Normalize integer-like field_id values in a polygon GeoPackage.")
    ap.add_argument("--input-vector", required=True)
    ap.add_argument("--output-vector", required=True)
    ap.add_argument("--field-id-field", default="field_id")
    ap.add_argument("--summary-json", default="")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)

    normalize_polygon_field_ids(
        input_vector=Path(args.input_vector).expanduser().resolve(),
        output_vector=Path(args.output_vector).expanduser().resolve(),
        field_id_field=str(args.field_id_field),
        summary_json=(Path(args.summary_json).expanduser().resolve() if str(args.summary_json or "").strip() else None),
        overwrite=bool(args.overwrite),
        verbose=bool(args.verbose),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
