from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path


TILE_RE = re.compile(r"(?i)(h\d{2}v\d{2})")


def _extract_tile_id(text: str) -> str:
    match = TILE_RE.search(str(text or ""))
    return match.group(1).lower() if match else ""


def _normalize_field_id_text(value) -> str:
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:  # noqa: BLE001
            pass
    if value is None:
        return ""
    if isinstance(value, float):
        if math.isnan(value):
            return ""
        if value.is_integer():
            return str(int(value))
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    if re.fullmatch(r"[+-]?\d+\.0+", text):
        return text.split(".", 1)[0]
    return text


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
        import pandas as pd
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("export_tile_polygon_geojson requires geopandas and pandas") from exc

    if not input_vector.exists():
        raise FileNotFoundError(f"input vector not found: {input_vector}")

    tile_id = _extract_tile_id(input_vector.name)
    if not tile_id:
        raise RuntimeError(f"unable to determine tile id from input name: {input_vector.name}")

    gdf = gpd.read_file(input_vector)
    if field_id_field not in gdf.columns:
        if gdf.empty:
            gdf[field_id_field] = pd.Series(dtype="object")
        else:
            raise RuntimeError(f"missing field id field: {field_id_field}")

    if gdf.empty:
        if gdf.crs is None:
            gdf = gdf.set_crs(source_crs, allow_override=True)
        gdf = gdf.copy()
        gdf["source_name"] = input_vector.name
        gdf["tile_id"] = tile_id
        gdf["tile_coord"] = tile_id
        gdf[field_id_field] = pd.Series(dtype="object")
        gdf["tile_field_id"] = pd.Series(dtype="object")

        reproj = gdf.to_crs(target_crs)
        preferred = ["tile_field_id", "tile_id", field_id_field, "tile_coord", "source_name", "FIPS", "STATEFP", "COUNTYFP", "county", "county_name_lsad", "field_area", "overlap_area", "county_overlap_pct", "county_match_count"]
        keep_cols = [c for c in preferred if c in reproj.columns]
        keep_cols.extend([c for c in reproj.columns if c not in keep_cols and c != "geometry"])
        keep_cols.append("geometry")
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
            "row_count": 0,
            "target_crs": target_crs,
            "columns": [str(col) for col in reproj.columns],
            "empty_input": True,
        }
        if summary_json is not None:
            summary_json.parent.mkdir(parents=True, exist_ok=True)
            summary_json.write_text(json.dumps(result, indent=2), encoding="utf-8")
        if verbose:
            print(json.dumps(result, indent=2))
        return result

    if field_id_field not in gdf.columns:
        raise RuntimeError(f"missing field id field: {field_id_field}")

    if gdf.crs is None:
        gdf = gdf.set_crs(source_crs, allow_override=True)

    if "source_name" not in gdf.columns:
        gdf["source_name"] = input_vector.name
    else:
        gdf["source_name"] = gdf["source_name"].map(lambda value: str(value).strip() if str(value).strip() else input_vector.name)
    gdf["tile_id"] = gdf["tile_id"].map(lambda value: str(value).strip().lower() if str(value).strip() else tile_id) if "tile_id" in gdf.columns else tile_id
    gdf["tile_coord"] = gdf["tile_coord"].map(lambda value: str(value).strip().lower() if str(value).strip() else tile_id) if "tile_coord" in gdf.columns else tile_id
    gdf[field_id_field] = gdf[field_id_field].map(_normalize_field_id_text)
    gdf["tile_field_id"] = gdf[field_id_field].map(lambda value: f"{tile_id}_{value}" if value else "")

    reproj = gdf.to_crs(target_crs)
    preferred = ["tile_field_id", "tile_id", field_id_field, "tile_coord", "source_name", "FIPS", "STATEFP", "COUNTYFP", "county", "county_name_lsad", "field_area", "overlap_area", "county_overlap_pct", "county_match_count"]
    keep_cols = [c for c in preferred if c in reproj.columns]
    keep_cols.extend([c for c in reproj.columns if c not in keep_cols and c != "geometry"])
    keep_cols.append("geometry")
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
