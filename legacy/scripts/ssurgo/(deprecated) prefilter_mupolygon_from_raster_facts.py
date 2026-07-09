#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any


def _resolve(path_text: str) -> Path:
    return Path(str(path_text or "")).expanduser().resolve()


def _vlog(verbose: bool, message: str) -> None:
    if verbose:
        print(f"[prefilter_mupolygon_from_raster_facts] {message}")


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:  # noqa: BLE001
            pass
    return str(value)


def _parse_states_csv(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for token in str(raw or "").replace(";", ",").split(","):
        state = str(token or "").strip().upper()
        if not state:
            continue
        if state in seen:
            continue
        seen.add(state)
        out.append(state)
    return out


def _parse_bounds(raw: str) -> tuple[float, float, float, float]:
    parts = [p.strip() for p in str(raw or "").replace(";", ",").split(",") if p.strip()]
    if len(parts) != 4:
        raise ValueError(f"invalid bounds value: {raw}")
    minx, miny, maxx, maxy = (float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3]))
    if maxx <= minx or maxy <= miny:
        raise ValueError(f"bounds must satisfy maxx>minx and maxy>miny: {raw}")
    return (minx, miny, maxx, maxy)


def _normalize_state_value(value: Any, state_col_name: str) -> str:
    text = re.sub(r"[^A-Za-z0-9]", "", _to_text(value).upper())
    if not text:
        return ""
    col_lower = str(state_col_name or "").strip().lower()
    if col_lower in {"areasymbol", "areasym", "area_symbol"}:
        return text[:2] if len(text) >= 2 else text
    if len(text) == 2:
        return text
    if len(text) >= 2 and text[:2].isalpha():
        return text[:2]
    return text


def _lookup_col_case_insensitive(cols: list[Any], target: str) -> str:
    by_lower = {str(c).lower(): str(c) for c in cols}
    return str(by_lower.get(str(target or "").strip().lower()) or "")


def _apply_state_filter(cand, *, states: list[str], state_field: str, verbose: bool):
    if not states:
        return cand, "", 0
    selected_col = ""
    if str(state_field or "").strip():
        selected_col = _lookup_col_case_insensitive(list(cand.columns), str(state_field))
    if not selected_col:
        for name in ("stusps", "state", "state_abbr", "statefp", "areasymbol", "areasym", "area_symbol"):
            selected_col = _lookup_col_case_insensitive(list(cand.columns), name)
            if selected_col:
                break
    if not selected_col:
        _vlog(verbose, f"state filter requested but no matching state column found; available={list(cand.columns)}")
        return cand, "", 0

    wanted = {str(s).upper() for s in states}
    before = int(len(cand))
    state_codes = cand[selected_col].map(lambda v: _normalize_state_value(v, selected_col))
    cand = cand[state_codes.isin(wanted)].copy()
    after = int(len(cand))
    _vlog(verbose, f"applied state filter column={selected_col} states={sorted(wanted)} rows_before={before} rows_after={after}")
    return cand, selected_col, max(0, before - after)


def prefilter_mupolygon_from_raster_facts(
    *,
    raster_facts_csv: str,
    bounds_field: str,
    crs_field: str,
    path_field: str,
    ssurgo_path: str,
    ssurgo_layer: str,
    mukey_field: str,
    states_csv: str,
    state_field: str,
    output_gpkg: str,
    output_layer: str,
    summary_json: str,
    verbose: bool,
) -> dict[str, Any]:
    try:
        import geopandas as gpd
        import pandas as pd
        from shapely.geometry import box
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("prefilter_mupolygon_from_raster_facts requires geopandas, pandas, shapely") from exc

    facts_path = _resolve(raster_facts_csv)
    if not facts_path.exists():
        raise FileNotFoundError(f"raster facts csv not found: {facts_path.as_posix()}")
    _vlog(verbose, f"reading raster facts csv={facts_path.as_posix()}")

    rows: list[dict[str, Any]] = []
    skipped_no_bounds = 0
    skipped_bad_bounds = 0
    seen: set[tuple[str, str, str]] = set()
    with facts_path.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            raise ValueError("raster facts csv has no header")
        for row in rdr:
            btxt = str((row or {}).get(bounds_field) or "").strip()
            if not btxt:
                skipped_no_bounds += 1
                continue
            try:
                bounds = _parse_bounds(btxt)
            except Exception:
                skipped_bad_bounds += 1
                continue
            rcrs = str((row or {}).get(crs_field) or "").strip()
            rpath = str((row or {}).get(path_field) or "").strip()
            key = (rpath, btxt, rcrs)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "source_path": rpath,
                    "bounds": btxt,
                    "crs": rcrs,
                    "geometry": box(bounds[0], bounds[1], bounds[2], bounds[3]),
                }
            )
    if not rows:
        raise RuntimeError("no usable raster-facts rows with bounds were found")

    # Group tile boxes by CRS and merge into a single GeoDataFrame for mask use.
    by_crs: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = str(row.get("crs") or "").strip()
        by_crs.setdefault(key, []).append(row)
    non_empty_crs = [k for k in by_crs.keys() if str(k).strip()]
    tile_crs = non_empty_crs[0] if non_empty_crs else ""
    tile_frames: list[Any] = []
    for crs_key, items in by_crs.items():
        frame = gpd.GeoDataFrame(items, geometry="geometry", crs=(crs_key or None))
        if tile_crs and crs_key and crs_key != tile_crs:
            frame = frame.to_crs(tile_crs)
        tile_frames.append(frame)
    tiles = gpd.GeoDataFrame(pd.concat(tile_frames, ignore_index=True), geometry="geometry", crs=(tile_crs or None))
    tiles = tiles[tiles.geometry.notna() & ~tiles.geometry.is_empty].copy()
    if tiles.empty:
        raise RuntimeError("tile extents from raster facts are empty")
    tile_bounds = tuple(float(v) for v in tiles.total_bounds.tolist())
    _vlog(verbose, f"tile extent boxes rows={len(tiles)} crs={tiles.crs} bbox={tile_bounds}")

    ssurgo_path_res = _resolve(ssurgo_path)
    if not ssurgo_path_res.exists():
        raise FileNotFoundError(f"ssurgo path not found: {ssurgo_path_res.as_posix()}")
    _vlog(verbose, f"reading SSURGO file={ssurgo_path_res.as_posix()} layer={ssurgo_layer}")

    # Read with bbox first for major IO reduction.
    read_kwargs: dict[str, Any] = {"layer": str(ssurgo_layer or "").strip()}
    if tiles.crs is not None:
        try:
            import pyproj

            with_bbox = tile_bounds
            # Transform bbox from tile CRS to SSURGO layer CRS.
            # First, read 1 row to detect SSURGO CRS without full materialization.
            sample = gpd.read_file(ssurgo_path_res, layer=str(ssurgo_layer or "").strip(), rows=1)
            ssurgo_crs = sample.crs
            if ssurgo_crs is not None and ssurgo_crs != tiles.crs:
                tr = pyproj.Transformer.from_crs(tiles.crs, ssurgo_crs, always_xy=True)
                minx, miny, maxx, maxy = with_bbox
                xs = [minx, minx, maxx, maxx]
                ys = [miny, maxy, miny, maxy]
                tx, ty = tr.transform(xs, ys)
                with_bbox = (float(min(tx)), float(min(ty)), float(max(tx)), float(max(ty)))
            read_kwargs["bbox"] = with_bbox
        except Exception:
            read_kwargs["bbox"] = tile_bounds

    ssurgo = gpd.read_file(ssurgo_path_res, **read_kwargs)
    if ssurgo.empty:
        raise RuntimeError("no SSURGO rows loaded from source with raster-facts tile bbox")

    by_lower = {str(c).lower(): c for c in ssurgo.columns}
    actual_mukey_col = by_lower.get(str(mukey_field).lower())
    if actual_mukey_col is None:
        raise ValueError(f"mukey field not found in SSURGO layer; requested={mukey_field}, available={list(ssurgo.columns)}")

    states = _parse_states_csv(states_csv)
    state_filter_column_used = ""
    state_filtered_rows = 0
    if states:
        ssurgo, state_filter_column_used, state_filtered_rows = _apply_state_filter(
            ssurgo,
            states=states,
            state_field=state_field,
            verbose=verbose,
        )
    if ssurgo.empty:
        raise RuntimeError("SSURGO became empty after state filter")

    ssurgo = ssurgo[[actual_mukey_col, "geometry"]].copy()
    if actual_mukey_col != mukey_field:
        ssurgo = ssurgo.rename(columns={actual_mukey_col: mukey_field})
    ssurgo[mukey_field] = ssurgo[mukey_field].map(_to_text)
    ssurgo = ssurgo[ssurgo.geometry.notna() & ~ssurgo.geometry.is_empty].copy()
    if ssurgo.crs is None:
        raise RuntimeError("SSURGO layer missing CRS")
    if tiles.crs is None:
        tiles = tiles.set_crs(ssurgo.crs)
    elif tiles.crs != ssurgo.crs:
        tiles = tiles.to_crs(ssurgo.crs)

    _vlog(verbose, f"running SSURGO x tile extent spatial join ssurgo_rows={len(ssurgo)} tile_rows={len(tiles)}")
    joined = gpd.sjoin(ssurgo[[mukey_field, "geometry"]], tiles[["geometry"]], how="inner", predicate="intersects")
    if joined.empty:
        raise RuntimeError("no SSURGO polygons intersect raster-facts tile extents")
    filtered = joined[[mukey_field, "geometry"]].copy()
    filtered = filtered[~filtered.index.duplicated(keep="first")]
    filtered = gpd.GeoDataFrame(filtered, geometry="geometry", crs=ssurgo.crs)

    output_path = _resolve(output_gpkg)
    summary_path = _resolve(summary_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        output_path.unlink()
    filtered.to_file(output_path, layer=str(output_layer or "ssurgo_mapunit"), driver="GPKG")
    _vlog(verbose, f"wrote prefiltered SSURGO gpkg={output_path.as_posix()} layer={output_layer} rows={len(filtered)}")

    summary = {
        "inputs": {
            "raster_facts_csv": facts_path.as_posix(),
            "bounds_field": bounds_field,
            "crs_field": crs_field,
            "path_field": path_field,
            "ssurgo_path": ssurgo_path_res.as_posix(),
            "ssurgo_layer": str(ssurgo_layer or ""),
            "mukey_field": str(mukey_field or ""),
            "states_csv": str(states_csv or ""),
            "state_field": str(state_field or ""),
            "state_filter_column_used": state_filter_column_used,
        },
        "counts": {
            "raster_facts_rows_total": len(rows),
            "raster_facts_rows_skipped_no_bounds": int(skipped_no_bounds),
            "raster_facts_rows_skipped_bad_bounds": int(skipped_bad_bounds),
            "tile_extent_rows": int(len(tiles)),
            "ssurgo_rows_loaded": int(len(ssurgo)),
            "state_filtered_rows": int(state_filtered_rows),
            "ssurgo_rows_intersecting_tiles": int(len(filtered)),
            "unique_mukey_count": int(filtered[mukey_field].nunique()),
        },
        "outputs": {
            "prefiltered_gpkg": output_path.as_posix(),
            "prefiltered_layer": str(output_layer or "ssurgo_mapunit"),
            "summary_json": summary_path.as_posix(),
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(description="Prefilter SSURGO MUPOLYGON by tile extents from filtered_raster_facts.csv")
    ap.add_argument("--raster-facts-csv", required=True)
    ap.add_argument("--bounds-field", default="bounds")
    ap.add_argument("--crs-field", default="crs")
    ap.add_argument("--path-field", default="relative_path")
    ap.add_argument("--ssurgo-path", required=True)
    ap.add_argument("--ssurgo-layer", default="MUPOLYGON")
    ap.add_argument("--mukey-field", default="mukey")
    ap.add_argument("--states-csv", default="")
    ap.add_argument("--state-field", default="areasymbol")
    ap.add_argument("--output-gpkg", required=True)
    ap.add_argument("--output-layer", default="ssurgo_mapunit")
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    summary = prefilter_mupolygon_from_raster_facts(
        raster_facts_csv=str(args.raster_facts_csv),
        bounds_field=str(args.bounds_field),
        crs_field=str(args.crs_field),
        path_field=str(args.path_field),
        ssurgo_path=str(args.ssurgo_path),
        ssurgo_layer=str(args.ssurgo_layer or "MUPOLYGON"),
        mukey_field=str(args.mukey_field or "mukey"),
        states_csv=str(args.states_csv or ""),
        state_field=str(args.state_field or ""),
        output_gpkg=str(args.output_gpkg),
        output_layer=str(args.output_layer or "ssurgo_mapunit"),
        summary_json=str(args.summary_json),
        verbose=bool(args.verbose),
    )
    if args.verbose:
        print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
