#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _to_scalar(value: Any):
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:  # noqa: BLE001
            return value
    return value


def export_field_polygons_ndjson(
    input_vector: Path,
    output_ndjson: Path,
    summary_json: Path,
    *,
    id_field: str = "tile_field_id",
    tile_field: str = "tile_id",
    field_field: str = "field_id",
    source_name_field: str = "source_name",
    verbose: bool = False,
) -> dict[str, Any]:
    try:
        import geopandas as gpd
        from shapely.geometry import mapping
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("export_field_polygons_ndjson requires geopandas and shapely") from exc

    if not input_vector.exists():
        raise FileNotFoundError(f"input vector not found: {input_vector}")

    gdf = gpd.read_file(input_vector)
    if gdf.empty:
        raise RuntimeError(f"input vector has no features: {input_vector}")
    if gdf.crs is None:
        raise RuntimeError("input vector missing CRS")
    gdf = gdf.to_crs("EPSG:4326")

    output_ndjson.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with output_ndjson.open("w", encoding="utf-8") as out:
        for _, row in gdf.iterrows():
            geom = row.get("geometry")
            if geom is None or getattr(geom, "is_empty", True):
                continue
            props: dict[str, Any] = {}
            tile_value = _to_scalar(row.get(tile_field)) if tile_field in row else None
            field_value = _to_scalar(row.get(field_field)) if field_field in row else None
            id_value = _to_scalar(row.get(id_field)) if id_field in row else None
            source_value = _to_scalar(row.get(source_name_field)) if source_name_field in row else None

            if id_value in (None, "") and tile_value not in (None, "") and field_value not in (None, ""):
                id_value = f"{tile_value}_{field_value}"

            if id_value not in (None, ""):
                props["tile_field_id"] = str(id_value)
                props["primary_key"] = str(id_value)
            if tile_value not in (None, ""):
                props["tile_id"] = str(tile_value)
            if field_value not in (None, ""):
                props["field_id"] = int(field_value) if str(field_value).isdigit() else field_value
            if source_value not in (None, ""):
                props["source_name"] = str(source_value)

            feature = {
                "type": "Feature",
                "geometry": mapping(geom),
                "properties": props,
            }
            out.write(json.dumps(feature, separators=(",", ":")) + "\n")
            written += 1

    summary = {
        "input_vector": input_vector.resolve().as_posix(),
        "output_ndjson": output_ndjson.resolve().as_posix(),
        "feature_count": int(written),
        "source_feature_count": int(len(gdf)),
        "crs": "EPSG:4326",
        "id_field": id_field,
        "tile_field": tile_field,
        "field_field": field_field,
        "source_name_field": source_name_field,
    }
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if verbose:
        print(
            f"[export_field_polygons_ndjson] input={input_vector.as_posix()} "
            f"written={written} output={output_ndjson.as_posix()}"
        )
    return summary


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Export field polygons vector to newline-delimited GeoJSON for web delivery.")
    ap.add_argument("--input-vector", required=True)
    ap.add_argument("--output-ndjson", required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--id-field", default="tile_field_id")
    ap.add_argument("--tile-field", default="tile_id")
    ap.add_argument("--field-field", default="field_id")
    ap.add_argument("--source-name-field", default="source_name")
    ap.add_argument("--verbose", action="store_true")
    args, unknown = ap.parse_known_args(argv)
    if unknown:
        print(f"[export_field_polygons_ndjson][WARN] ignoring unknown args: {' '.join(unknown)}")

    export_field_polygons_ndjson(
        input_vector=Path(args.input_vector).expanduser().resolve(),
        output_ndjson=Path(args.output_ndjson).expanduser().resolve(),
        summary_json=Path(args.summary_json).expanduser().resolve(),
        id_field=str(args.id_field),
        tile_field=str(args.tile_field),
        field_field=str(args.field_field),
        source_name_field=str(args.source_name_field),
        verbose=bool(args.verbose),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
