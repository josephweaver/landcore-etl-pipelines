from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


TILE_RE = re.compile(r"(?i)(h\d{2}v\d{2})")


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    return str(value).strip()


def _extract_tile_id(value: str) -> str:
    match = TILE_RE.search(_to_text(value))
    return match.group(1).lower() if match else ""


def assign_polygon_fips(
    *,
    input_vector: Path,
    county_path: Path,
    output_vector: Path,
    summary_json: Path,
    tile: str = "",
    field_id_field: str = "field_id",
    tile_id_field: str = "tile_id",
    tile_coord_field: str = "tile_coord",
    tile_field_id_field: str = "tile_field_id",
    county_geoid_field: str = "GEOID",
    county_name_field: str = "NAME",
    county_name_lsad_field: str = "NAMELSAD",
    statefp_field: str = "STATEFP",
    countyfp_field: str = "COUNTYFP",
    overwrite: bool = False,
    verbose: bool = False,
) -> dict[str, Any]:
    try:
        import geopandas as gpd
        import pandas as pd
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("assign_polygon_fips requires geopandas and pandas") from exc

    if not input_vector.exists():
        raise FileNotFoundError(f"input vector not found: {input_vector}")
    if not county_path.exists():
        raise FileNotFoundError(f"county path not found: {county_path}")
    if output_vector.exists() and not overwrite:
        raise FileExistsError(f"output vector exists and overwrite not requested: {output_vector}")

    field_gdf = gpd.read_file(input_vector)
    county_gdf = gpd.read_file(county_path)
    if field_gdf.empty:
        raise RuntimeError("input vector is empty")
    if county_gdf.empty:
        raise RuntimeError("county dataset is empty")
    if field_gdf.crs is None:
        raise RuntimeError("input vector missing CRS")
    if county_gdf.crs is None:
        raise RuntimeError("county dataset missing CRS")
    if field_id_field not in field_gdf.columns:
        raise RuntimeError(f"missing field id field: {field_id_field}")
    if county_geoid_field not in county_gdf.columns:
        raise RuntimeError(f"county dataset missing required field: {county_geoid_field}")

    tile_id = _to_text(tile).lower() or _extract_tile_id(input_vector.name)
    if not tile_id:
        raise RuntimeError(f"unable to determine tile id from tile arg or input name: {input_vector.name}")

    field_gdf = field_gdf[field_gdf.geometry.notna() & ~field_gdf.geometry.is_empty].copy()
    if field_gdf.empty:
        raise RuntimeError("input vector has no usable geometries")

    field_gdf[field_id_field] = field_gdf[field_id_field].map(_to_text)
    field_gdf = field_gdf[field_gdf[field_id_field].astype(str).str.len() > 0].copy()
    if field_gdf.empty:
        raise RuntimeError("input vector has no rows with field ids")

    field_gdf[tile_id_field] = tile_id
    field_gdf[tile_coord_field] = tile_id
    field_gdf[tile_field_id_field] = field_gdf[field_id_field].map(lambda value: f"{tile_id}_{value}" if value else "")
    field_gdf["field_area"] = field_gdf.geometry.area.astype(float)

    if county_gdf.crs != field_gdf.crs:
        county_gdf = county_gdf.to_crs(field_gdf.crs)
    county_keep_cols = [
        c
        for c in [county_geoid_field, county_name_field, county_name_lsad_field, statefp_field, countyfp_field]
        if c in county_gdf.columns
    ]
    county_gdf = county_gdf[county_keep_cols + ["geometry"]].copy()
    county_gdf = county_gdf[county_gdf.geometry.notna() & ~county_gdf.geometry.is_empty].copy()
    if county_gdf.empty:
        raise RuntimeError("county dataset has no usable geometries")

    field_bounds = field_gdf.total_bounds
    county_gdf = county_gdf.cx[field_bounds[0] : field_bounds[2], field_bounds[1] : field_bounds[3]].copy()
    if county_gdf.empty:
        raise RuntimeError("county dataset has no geometries overlapping the field extent")

    intersections = gpd.overlay(field_gdf[[field_id_field, tile_field_id_field, "field_area", "geometry"]], county_gdf, how="intersection", keep_geom_type=False)
    intersections = intersections[intersections.geometry.notna() & ~intersections.geometry.is_empty].copy()
    if intersections.empty:
        raise RuntimeError("no field/county intersections were produced")

    intersections["overlap_area"] = intersections.geometry.area.astype(float)
    intersections = intersections[intersections["overlap_area"] > 0].copy()
    if intersections.empty:
        raise RuntimeError("field/county intersections all had non-positive overlap area")

    intersections["county_overlap_pct"] = (
        intersections["overlap_area"] / intersections["field_area"].where(intersections["field_area"] > 0)
    ) * 100.0
    intersections.sort_values(
        by=[field_id_field, "overlap_area", county_geoid_field],
        ascending=[True, False, True],
        inplace=True,
    )

    match_counts = (
        intersections.groupby(field_id_field)[county_geoid_field]
        .nunique()
        .rename("county_match_count")
        .reset_index()
    )
    best = intersections.drop_duplicates(subset=[field_id_field], keep="first").copy()
    best = best.merge(match_counts, on=field_id_field, how="left")

    keep_join_cols = [field_id_field, tile_field_id_field, "field_area", "overlap_area", "county_overlap_pct", "county_match_count"]
    for col in [county_geoid_field, county_name_field, county_name_lsad_field, statefp_field, countyfp_field]:
        if col in best.columns and col not in keep_join_cols:
            keep_join_cols.append(col)
    join_df = pd.DataFrame(best[keep_join_cols]).copy()
    rename_map = {}
    if county_geoid_field in join_df.columns:
        rename_map[county_geoid_field] = "FIPS"
    if county_name_field in join_df.columns:
        rename_map[county_name_field] = "county"
    if county_name_lsad_field in join_df.columns:
        rename_map[county_name_lsad_field] = "county_name_lsad"
    join_df.rename(columns=rename_map, inplace=True)

    out_gdf = field_gdf.merge(join_df, on=[field_id_field, tile_field_id_field, "field_area"], how="left")
    output_vector.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    if output_vector.exists() and overwrite:
        output_vector.unlink()
    out_gdf.to_file(output_vector, driver="GPKG")

    summary = {
        "input_vector": input_vector.as_posix(),
        "county_path": county_path.as_posix(),
        "output_vector": output_vector.as_posix(),
        "tile": tile_id,
        "input_row_count": int(len(field_gdf)),
        "intersection_row_count": int(len(intersections)),
        "output_row_count": int(len(out_gdf)),
        "ambiguous_field_count": int(sum(1 for x in out_gdf.get("county_match_count", []) if x and float(x) > 1)),
        "columns": [str(col) for col in out_gdf.columns],
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if verbose:
        print(json.dumps(summary, indent=2))
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(description="Assign county FIPS attributes directly onto per-tile YanRoy polygon GeoPackages.")
    ap.add_argument("--input-vector", required=True)
    ap.add_argument("--county-path", required=True)
    ap.add_argument("--output-vector", required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--tile", default="")
    ap.add_argument("--field-id-field", default="field_id")
    ap.add_argument("--tile-id-field", default="tile_id")
    ap.add_argument("--tile-coord-field", default="tile_coord")
    ap.add_argument("--tile-field-id-field", default="tile_field_id")
    ap.add_argument("--county-geoid-field", default="GEOID")
    ap.add_argument("--county-name-field", default="NAME")
    ap.add_argument("--county-name-lsad-field", default="NAMELSAD")
    ap.add_argument("--statefp-field", default="STATEFP")
    ap.add_argument("--countyfp-field", default="COUNTYFP")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    assign_polygon_fips(
        input_vector=Path(str(args.input_vector)).expanduser().resolve(),
        county_path=Path(str(args.county_path)).expanduser().resolve(),
        output_vector=Path(str(args.output_vector)).expanduser().resolve(),
        summary_json=Path(str(args.summary_json)).expanduser().resolve(),
        tile=str(args.tile or ""),
        field_id_field=str(args.field_id_field),
        tile_id_field=str(args.tile_id_field),
        tile_coord_field=str(args.tile_coord_field),
        tile_field_id_field=str(args.tile_field_id_field),
        county_geoid_field=str(args.county_geoid_field),
        county_name_field=str(args.county_name_field),
        county_name_lsad_field=str(args.county_name_lsad_field),
        statefp_field=str(args.statefp_field),
        countyfp_field=str(args.countyfp_field),
        overwrite=bool(args.overwrite),
        verbose=bool(args.verbose),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
