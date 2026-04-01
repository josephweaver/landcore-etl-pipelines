#!/usr/bin/env python3
from __future__ import annotations

import argparse
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


def _write_empty_outputs(
    *,
    output_csv: Path,
    summary_json: Path,
    tile: str,
    field_boundary_path: Path,
    county_path: Path,
    field_id_field: str,
    output_tile_field_id_field: str,
) -> dict[str, Any]:
    import csv

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        field_id_field,
        output_tile_field_id_field,
        "tile_coord",
        "yanroy_field_id",
        "FIPS",
        "STATEFP",
        "COUNTYFP",
        "county",
        "county_name_lsad",
        "field_area",
        "overlap_area",
        "county_overlap_pct",
        "county_match_count",
    ]
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

    summary = {
        "field_boundary_path": field_boundary_path.as_posix(),
        "county_path": county_path.as_posix(),
        "output_csv": output_csv.as_posix(),
        "tile": str(tile or "").strip().lower(),
        "field_boundary_input_count": 0,
        "dissolved_field_count": 0,
        "intersection_row_count": 0,
        "output_row_count": 0,
        "ambiguous_field_count": 0,
        "empty_tile": True,
        "empty_reason": "no field rows after identifier/tile filtering",
        "field_id_field": field_id_field,
        "output_tile_field_id_field": output_tile_field_id_field,
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def build_field_fips(
    *,
    field_boundary_path: Path,
    county_path: Path,
    output_csv: Path,
    summary_json: Path,
    tile: str,
    field_id_field: str,
    tile_field_id_field: str,
    output_tile_field_id_field: str,
    tile_coord_field: str,
    yanroy_field_id_field: str,
    county_geoid_field: str,
    county_name_field: str,
    county_name_lsad_field: str,
    statefp_field: str,
    countyfp_field: str,
    verbose: bool = False,
) -> dict[str, Any]:
    try:
        import geopandas as gpd
        import pandas as pd
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("assign_field_fips requires geopandas and pandas") from exc

    if not field_boundary_path.exists():
        raise FileNotFoundError(f"field boundary path not found: {field_boundary_path}")
    if not county_path.exists():
        raise FileNotFoundError(f"county path not found: {county_path}")

    field_gdf = gpd.read_file(field_boundary_path)
    county_gdf = gpd.read_file(county_path)
    if field_gdf.empty:
        raise RuntimeError("field boundary dataset is empty")
    if county_gdf.empty:
        raise RuntimeError("county dataset is empty")
    if field_gdf.crs is None:
        raise RuntimeError("field boundary dataset missing CRS")
    if county_gdf.crs is None:
        raise RuntimeError("county dataset missing CRS")
    tile_norm = _to_text(tile).lower()

    required_field_cols = [field_id_field, tile_field_id_field]
    missing_field_cols = [c for c in required_field_cols if c not in field_gdf.columns]
    if missing_field_cols:
        raise ValueError(f"field boundary dataset missing required columns: {missing_field_cols}")

    required_county_cols = [county_geoid_field]
    missing_county_cols = [c for c in required_county_cols if c not in county_gdf.columns]
    if missing_county_cols:
        raise ValueError(f"county dataset missing required columns: {missing_county_cols}")

    keep_field_cols = [
        c
        for c in [field_id_field, tile_field_id_field, tile_coord_field, yanroy_field_id_field]
        if c in field_gdf.columns
    ]
    field_gdf = field_gdf[keep_field_cols + ["geometry"]].copy()
    field_gdf = field_gdf[field_gdf.geometry.notna() & ~field_gdf.geometry.is_empty].copy()
    if field_gdf.empty:
        raise RuntimeError("field boundary dataset has no usable geometries")

    field_gdf[field_id_field] = field_gdf[field_id_field].map(_to_text)
    field_gdf[tile_field_id_field] = field_gdf[tile_field_id_field].map(_to_text)
    field_gdf = field_gdf[
        (field_gdf[field_id_field].astype(str).str.len() > 0)
        & (field_gdf[tile_field_id_field].astype(str).str.len() > 0)
    ].copy()
    if tile_norm:
        if tile_coord_field not in field_gdf.columns:
            raise ValueError(f"field boundary dataset missing tile coordinate column: {tile_coord_field}")
        field_gdf[tile_coord_field] = field_gdf[tile_coord_field].map(_to_text).str.lower()
        field_gdf = field_gdf[field_gdf[tile_coord_field] == tile_norm].copy()
    if field_gdf.empty:
        return _write_empty_outputs(
            output_csv=output_csv,
            summary_json=summary_json,
            tile=tile_norm,
            field_boundary_path=field_boundary_path,
            county_path=county_path,
            field_id_field=field_id_field,
            output_tile_field_id_field=output_tile_field_id_field,
        )

    dissolve_fields = [field_id_field, tile_field_id_field]
    agg_map: dict[str, str] = {}
    for opt_col in [tile_coord_field, yanroy_field_id_field]:
        if opt_col in field_gdf.columns:
            agg_map[opt_col] = "first"
    dissolved = field_gdf.dissolve(by=dissolve_fields, as_index=False, aggfunc=agg_map or "first")
    dissolved = dissolved[dissolved.geometry.notna() & ~dissolved.geometry.is_empty].copy()
    if dissolved.empty:
        raise RuntimeError("dissolved field boundary dataset is empty")
    dissolved["field_area"] = dissolved.geometry.area.astype(float)

    if county_gdf.crs != dissolved.crs:
        county_gdf = county_gdf.to_crs(dissolved.crs)
    county_keep_cols = [
        c
        for c in [county_geoid_field, county_name_field, county_name_lsad_field, statefp_field, countyfp_field]
        if c in county_gdf.columns
    ]
    county_gdf = county_gdf[county_keep_cols + ["geometry"]].copy()
    county_gdf = county_gdf[county_gdf.geometry.notna() & ~county_gdf.geometry.is_empty].copy()
    if county_gdf.empty:
        raise RuntimeError("county dataset has no usable geometries")

    field_bounds = dissolved.total_bounds
    county_gdf = county_gdf.cx[field_bounds[0] : field_bounds[2], field_bounds[1] : field_bounds[3]].copy()
    if county_gdf.empty:
        raise RuntimeError("county dataset has no geometries overlapping the field boundary extent")

    intersections = gpd.overlay(dissolved, county_gdf, how="intersection", keep_geom_type=False)
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

    field_match_counts = (
        intersections.groupby(field_id_field)[county_geoid_field]
        .nunique()
        .rename("county_match_count")
        .reset_index()
    )
    best = intersections.drop_duplicates(subset=[field_id_field], keep="first").copy()
    best = best.merge(field_match_counts, on=field_id_field, how="left")

    output_columns = [field_id_field, tile_field_id_field]
    for opt_col in [tile_coord_field, yanroy_field_id_field]:
        if opt_col in best.columns:
            output_columns.append(opt_col)
    for county_col in [county_geoid_field, statefp_field, countyfp_field, county_name_field, county_name_lsad_field]:
        if county_col in best.columns and county_col not in output_columns:
            output_columns.append(county_col)
    output_columns.extend(["field_area", "overlap_area", "county_overlap_pct", "county_match_count"])

    output_df = pd.DataFrame(best[output_columns]).copy()
    rename_map = {}
    if tile_field_id_field in output_df.columns:
        rename_map[tile_field_id_field] = output_tile_field_id_field
    if county_geoid_field in output_df.columns:
        rename_map[county_geoid_field] = "FIPS"
    if county_name_field in output_df.columns:
        rename_map[county_name_field] = "county"
    if county_name_lsad_field in output_df.columns:
        rename_map[county_name_lsad_field] = "county_name_lsad"
    output_df.rename(columns=rename_map, inplace=True)
    output_df.sort_values(by=[output_tile_field_id_field], inplace=True)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_csv, index=False, encoding="utf-8")

    ambiguous_fields = int(sum(1 for x in output_df.get("county_match_count", []) if float(x) > 1))
    summary = {
        "field_boundary_path": field_boundary_path.as_posix(),
        "county_path": county_path.as_posix(),
        "output_csv": output_csv.as_posix(),
        "tile": tile_norm,
        "field_boundary_input_count": int(len(field_gdf)),
        "dissolved_field_count": int(len(dissolved)),
        "intersection_row_count": int(len(intersections)),
        "output_row_count": int(len(output_df)),
        "ambiguous_field_count": ambiguous_fields,
        "field_id_field": field_id_field,
        "tile_field_id_field": tile_field_id_field,
        "output_tile_field_id_field": output_tile_field_id_field,
        "county_geoid_field": county_geoid_field,
        "county_name_field": county_name_field,
        "county_name_lsad_field": county_name_lsad_field,
        "statefp_field": statefp_field,
        "countyfp_field": countyfp_field,
        "field_crs": str(dissolved.crs),
        "county_crs": str(county_gdf.crs),
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if verbose:
        print(json.dumps(summary, indent=2))
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(description="Assign one county FIPS to each YanRoy tile_field_ID using maximum overlap area.")
    ap.add_argument("--field-boundary-path", required=True)
    ap.add_argument("--county-path", required=True)
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--tile", default="")
    ap.add_argument("--field-id-field", default="field_id")
    ap.add_argument("--tile-field-id-field", default="legacy_tile_field_id")
    ap.add_argument("--output-tile-field-id-field", default="tile_field_ID")
    ap.add_argument("--tile-coord-field", default="tile_coord")
    ap.add_argument("--yanroy-field-id-field", default="yanroy_field_id")
    ap.add_argument("--county-geoid-field", default="GEOID")
    ap.add_argument("--county-name-field", default="NAME")
    ap.add_argument("--county-name-lsad-field", default="NAMELSAD")
    ap.add_argument("--statefp-field", default="STATEFP")
    ap.add_argument("--countyfp-field", default="COUNTYFP")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    build_field_fips(
        field_boundary_path=Path(str(args.field_boundary_path)).expanduser().resolve(),
        county_path=Path(str(args.county_path)).expanduser().resolve(),
        output_csv=Path(str(args.output_csv)).expanduser().resolve(),
        summary_json=Path(str(args.summary_json)).expanduser().resolve(),
        tile=str(args.tile or ""),
        field_id_field=str(args.field_id_field),
        tile_field_id_field=str(args.tile_field_id_field),
        output_tile_field_id_field=str(args.output_tile_field_id_field),
        tile_coord_field=str(args.tile_coord_field),
        yanroy_field_id_field=str(args.yanroy_field_id_field),
        county_geoid_field=str(args.county_geoid_field),
        county_name_field=str(args.county_name_field),
        county_name_lsad_field=str(args.county_name_lsad_field),
        statefp_field=str(args.statefp_field),
        countyfp_field=str(args.countyfp_field),
        verbose=bool(args.verbose),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
