#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

TILE_RE = re.compile(r"(?i)(h\d{2}v\d{2})")


def _resolve(path_text: str) -> Path:
    return Path(path_text).expanduser().resolve()


def _normalize_text(value) -> str:
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:  # noqa: BLE001
            pass
    return str(value or "").strip()


def _extract_tile_id(source_name: str) -> str:
    match = TILE_RE.search(_normalize_text(source_name))
    return match.group(1).lower() if match else ""


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Normalize YanRoy field polygons so downstream jobs can rely on tile_id and tile_field_id."
    )
    ap.add_argument("--input-vector", required=True)
    ap.add_argument("--output-vector", required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--source-name-field", default="source_name")
    ap.add_argument("--field-id-field", default="field_id")
    ap.add_argument("--tile-id-field", default="tile_id")
    ap.add_argument("--tile-field-id-field", default="tile_field_id")
    ap.add_argument("--tile-coord-field", default="tile_coord")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    try:
        import geopandas as gpd
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("normalize_field_polygons requires geopandas") from exc

    input_vector = _resolve(args.input_vector)
    output_vector = _resolve(args.output_vector)
    summary_json = _resolve(args.summary_json)

    if not input_vector.exists():
        raise FileNotFoundError(f"input vector not found: {input_vector}")
    if output_vector.exists() and not args.overwrite:
        raise FileExistsError(f"output vector exists and overwrite not requested: {output_vector}")

    gdf = gpd.read_file(input_vector)
    if gdf.empty:
        raise RuntimeError(f"input vector has no features: {input_vector}")
    if args.source_name_field not in gdf.columns:
        raise RuntimeError(f"missing source name field: {args.source_name_field}")
    if args.field_id_field not in gdf.columns:
        raise RuntimeError(f"missing field id field: {args.field_id_field}")

    tile_ids: list[str] = []
    tile_field_ids: list[str] = []
    missing_tile_rows = 0
    missing_field_rows = 0

    for _, row in gdf.iterrows():
        source_name = _normalize_text(row.get(args.source_name_field))
        field_id = _normalize_text(row.get(args.field_id_field))
        tile_id = _extract_tile_id(source_name)
        if not tile_id:
            missing_tile_rows += 1
        if not field_id:
            missing_field_rows += 1
        tile_ids.append(tile_id)
        tile_field_ids.append(f"{tile_id}_{field_id}" if tile_id and field_id else "")

    gdf[args.tile_id_field] = tile_ids
    gdf[args.tile_coord_field] = tile_ids
    gdf[args.tile_field_id_field] = tile_field_ids

    output_vector.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    if output_vector.exists() and args.overwrite:
        output_vector.unlink()

    gdf.to_file(output_vector, driver="GPKG")

    summary = {
        "input_vector": input_vector.as_posix(),
        "output_vector": output_vector.as_posix(),
        "row_count": int(len(gdf)),
        "missing_tile_rows": int(missing_tile_rows),
        "missing_field_rows": int(missing_field_rows),
        "missing_tile_field_id_rows": int(sum(1 for value in tile_field_ids if not value)),
        "distinct_tiles": int(len({value for value in tile_ids if value})),
        "distinct_tile_field_ids": int(len({value for value in tile_field_ids if value})),
        "columns": [str(col) for col in gdf.columns],
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if args.verbose:
        print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
