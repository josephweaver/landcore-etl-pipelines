from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


TILE_RE = re.compile(r"(?i)(h\d{2}v\d{2})")


def _extract_tile_id(text: str) -> str:
    match = TILE_RE.search(str(text or ""))
    return match.group(1).lower() if match else ""


def export_tile_polygon_geojson(
    input_vector: Path,
    output_dir: Path,
    summary_json: Path | None = None,
    field_id_field: str = "field_id",
    source_crs: str = "EPSG:5070",
    target_crs: str = "EPSG:4326",
    verbose: bool = False,
) -> dict:
    try:
        import geopandas as gpd
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("export_tile_polygon_geojson requires geopandas") from exc

    if not input_vector.exists():
        raise FileNotFoundError(f"input vector not found: {input_vector}")

    tile_id = _extract_tile_id(input_vector.name)
    if not tile_id:
        raise RuntimeError(f"unable to determine tile id from input name: {input_vector.name}")

    gdf = gpd.read_file(input_vector)
    if gdf.empty:
        raise RuntimeError(f"input vector has no features: {input_vector}")
    if field_id_field not in gdf.columns:
        raise RuntimeError(f"missing field id field: {field_id_field}")

    if gdf.crs is None:
        gdf = gdf.set_crs(source_crs, allow_override=True)

    gdf["source_name"] = input_vector.name
    gdf["tile_id"] = tile_id
    gdf["tile_coord"] = tile_id
    gdf["tile_field_id"] = gdf[field_id_field].map(lambda value: f"{tile_id}_{str(value).strip()}" if str(value).strip() else "")

    reproj = gdf.to_crs(target_crs)
    keep_cols = [c for c in ["tile_field_id", "tile_id", field_id_field, "tile_coord", "source_name", "geometry"] if c in reproj.columns]
    reproj = reproj[keep_cols]

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{tile_id}.geojson"
    if output_path.exists():
        output_path.unlink()
    reproj.to_file(output_path, driver="GeoJSON")

    result = {
        "input_vector": input_vector.resolve().as_posix(),
        "output_geojson": output_path.resolve().as_posix(),
        "tile_id": tile_id,
        "row_count": int(len(reproj)),
        "target_crs": target_crs,
        "columns": [str(col) for col in reproj.columns],
    }
    if summary_json is not None:
        summary_json.parent.mkdir(parents=True, exist_ok=True)
        summary_json.write_text(json.dumps(result, indent=2), encoding="utf-8")
    if verbose:
        print(json.dumps(result, indent=2))
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Export one per-tile YanRoy polygon GeoJSON in EPSG:4326.")
    ap.add_argument("--input-vector", required=True)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--summary-json", default="")
    ap.add_argument("--field-id-field", default="field_id")
    ap.add_argument("--source-crs", default="EPSG:5070")
    ap.add_argument("--target-crs", default="EPSG:4326")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)

    export_tile_polygon_geojson(
        input_vector=Path(args.input_vector).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        summary_json=(Path(args.summary_json).expanduser().resolve() if str(args.summary_json or "").strip() else None),
        field_id_field=str(args.field_id_field),
        source_crs=str(args.source_crs),
        target_crs=str(args.target_crs),
        verbose=bool(args.verbose),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
