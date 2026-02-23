#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any


TILE_RE = re.compile(r"^[A-Za-z]\d{2}[A-Za-z]\d{2}$")


def _resolve(path_text: str) -> Path:
    p = Path(str(path_text or "")).expanduser()
    # Do not canonicalize absolute paths here; on HPCC, resolving symlinked
    # mount aliases (e.g. /mnt/scratch -> /mnt/gs21/scratch) can yield
    # non-existent canonical paths even when the original alias is valid.
    if p.is_absolute():
        return p
    return (Path.cwd() / p).resolve()


def _vlog(verbose: bool, message: str) -> None:
    if verbose:
        print(f"[build_tile_mukey_map] {message}")


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:  # noqa: BLE001
            pass
    return str(value)


def _lookup_col_case_insensitive(cols: list[Any], target: str) -> str:
    by_lower = {str(c).lower(): str(c) for c in cols}
    return str(by_lower.get(str(target or "").strip().lower()) or "")


def _parse_list_csv(raw: str, *, upper: bool = False) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for token in str(raw or "").replace(";", ",").split(","):
        t = str(token or "").strip()
        if not t:
            continue
        if upper:
            t = t.upper()
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


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


def _apply_state_filter(cand, *, states_csv: str, state_field: str, verbose: bool):
    states = _parse_list_csv(states_csv, upper=True)
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

    wanted = set(states)
    before = int(len(cand))
    state_codes = cand[selected_col].map(lambda v: _normalize_state_value(v, selected_col))
    cand = cand[state_codes.isin(wanted)].copy()
    after = int(len(cand))
    _vlog(verbose, f"applied state filter column={selected_col} states={sorted(wanted)} rows_before={before} rows_after={after}")
    return cand, selected_col, max(0, before - after)


def _derive_tile(relative_path: str, tile_prefix_len: int) -> str:
    rel = str(relative_path or "").strip().replace("\\", "/")
    if not rel:
        return ""
    if tile_prefix_len <= 0:
        tile_prefix_len = 6
    token = rel[:tile_prefix_len].lower()
    return token if TILE_RE.match(token) else ""


def _list_layers(path: Path) -> list[str]:
    names: list[str] = []
    try:
        import pyogrio  # type: ignore

        raw = pyogrio.list_layers(path.as_posix())
        for row in list(raw or []):
            if isinstance(row, (list, tuple)) and row:
                name = str(row[0] or "").strip()
            else:
                name = str(row or "").strip()
            if name:
                names.append(name)
    except Exception:  # noqa: BLE001
        try:
            import fiona  # type: ignore

            for name in list(fiona.listlayers(path.as_posix()) or []):
                text = str(name or "").strip()
                if text:
                    names.append(text)
        except Exception:  # noqa: BLE001
            pass
    # De-dup case-insensitively while preserving order.
    out: list[str] = []
    seen: set[str] = set()
    for name in names:
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def _read_ssurgo_with_layer_fallback(gpd, *, path: Path, layer: str, bbox: tuple[float, float, float, float], verbose: bool):
    available = _list_layers(path)
    requested = str(layer or "").strip()
    candidates: list[str] = []
    for name in [requested, "MUPOLYGON", "mupolygon", "ssurgo_mapunit", "SSURGO_MAPUNIT"]:
        if str(name or "").strip():
            candidates.append(str(name).strip())
    for name in available:
        low = name.lower()
        if "mupolygon" in low or "mapunit" in low:
            candidates.append(name)
    # De-dup case-insensitively while preserving order.
    dedup: list[str] = []
    seen: set[str] = set()
    for name in candidates:
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        dedup.append(name)
    errors: list[str] = []
    for cand in dedup:
        try:
            _vlog(verbose, f"trying SSURGO layer={cand}")
            df = gpd.read_file(path, layer=cand, bbox=bbox)
            return df, cand, available
        except Exception as exc:  # noqa: BLE001
            if len(errors) < 8:
                errors.append(f"{cand}: {exc}")
    avail_preview = ", ".join(available[:30]) if available else "<unavailable>"
    err_preview = "; ".join(errors) if errors else "no layer read attempts succeeded"
    raise RuntimeError(
        f"unable to read SSURGO polygon layer from {path.as_posix()}; "
        f"requested={requested or '<none>'}; available={avail_preview}; errors={err_preview}"
    )


def build_tile_mukey_map(
    *,
    filtered_csv: str,
    relative_path_field: str,
    minx_field: str,
    miny_field: str,
    maxx_field: str,
    maxy_field: str,
    tile_prefix_len: int,
    tiles_csv: str,
    ssurgo_path: str,
    ssurgo_layer: str,
    mukey_field: str,
    states_csv: str,
    state_field: str,
    output_tile_mukey_csv: str,
    output_tile_counts_csv: str,
    summary_json: str,
    verbose: bool,
) -> dict[str, Any]:
    try:
        import geopandas as gpd
        import pandas as pd
        from shapely.geometry import box
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("build_tile_mukey_map requires geopandas, pandas, shapely") from exc

    tiles_wanted = set(_parse_list_csv(tiles_csv, upper=False))
    facts_path = _resolve(filtered_csv)
    if not facts_path.exists():
        raise FileNotFoundError(f"filtered csv not found: {facts_path.as_posix()}")

    rows: list[dict[str, Any]] = []
    skipped_bad_tile = 0
    skipped_not_selected = 0
    skipped_bad_bounds = 0
    seen: set[tuple[str, float, float, float, float]] = set()
    with facts_path.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            raise ValueError("filtered csv has no header")
        for row in rdr:
            src_rel = str((row or {}).get(relative_path_field) or "").strip()
            tile = _derive_tile(src_rel, tile_prefix_len)
            if not tile:
                skipped_bad_tile += 1
                continue
            if tiles_wanted and tile not in tiles_wanted:
                skipped_not_selected += 1
                continue
            try:
                minx = float(str((row or {}).get(minx_field) or "").strip())
                miny = float(str((row or {}).get(miny_field) or "").strip())
                maxx = float(str((row or {}).get(maxx_field) or "").strip())
                maxy = float(str((row or {}).get(maxy_field) or "").strip())
            except Exception:
                skipped_bad_bounds += 1
                continue
            if maxx <= minx or maxy <= miny:
                skipped_bad_bounds += 1
                continue
            key = (tile, minx, miny, maxx, maxy)
            if key in seen:
                continue
            seen.add(key)
            rows.append({"tile": tile, "geometry": box(minx, miny, maxx, maxy)})
    if not rows:
        raise RuntimeError("no usable filtered.csv rows matched tile selection")

    # filtered.csv bounds are produced in EPSG:5070 for this workflow.
    tile_boxes = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:5070")
    tile_boxes = tile_boxes[tile_boxes.geometry.notna() & ~tile_boxes.geometry.is_empty].copy()
    if tile_boxes.empty:
        raise RuntimeError("tile boxes are empty after filtering")
    _vlog(verbose, f"tile boxes rows={len(tile_boxes)} unique_tiles={tile_boxes['tile'].nunique()}")

    ssurgo_path_res = _resolve(ssurgo_path)
    if not ssurgo_path_res.exists():
        raise FileNotFoundError(f"ssurgo path not found: {ssurgo_path_res.as_posix()}")

    # Use global bbox from selected tile boxes to reduce SSURGO IO.
    tb = tuple(float(v) for v in tile_boxes.total_bounds.tolist())
    ssurgo, ssurgo_layer_used, available_layers = _read_ssurgo_with_layer_fallback(
        gpd,
        path=ssurgo_path_res,
        layer=str(ssurgo_layer or "").strip(),
        bbox=tb,
        verbose=verbose,
    )
    if verbose:
        _vlog(
            verbose,
            f"loaded SSURGO layer={ssurgo_layer_used} rows={len(ssurgo)} "
            f"available_layer_count={len(available_layers)}",
        )
    if ssurgo.empty:
        raise RuntimeError("no SSURGO rows loaded for selected tile bbox")

    by_lower = {str(c).lower(): c for c in ssurgo.columns}
    actual_mukey_col = by_lower.get(str(mukey_field).lower())
    if actual_mukey_col is None:
        raise ValueError(f"mukey field not found in SSURGO layer; requested={mukey_field}, available={list(ssurgo.columns)}")

    ssurgo, state_filter_column_used, state_filtered_rows = _apply_state_filter(
        ssurgo,
        states_csv=states_csv,
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
    if tile_boxes.crs != ssurgo.crs:
        tile_boxes = tile_boxes.to_crs(ssurgo.crs)

    joined = gpd.sjoin(ssurgo[[mukey_field, "geometry"]], tile_boxes[["tile", "geometry"]], how="inner", predicate="intersects")
    if joined.empty:
        raise RuntimeError("no SSURGO polygons intersect selected tile boxes")

    pairs = joined[["tile", mukey_field]].copy()
    pairs[mukey_field] = pairs[mukey_field].astype(str).str.strip()
    pairs = pairs[pairs[mukey_field] != ""].copy()
    pairs = pairs.drop_duplicates(subset=["tile", mukey_field]).sort_values(["tile", mukey_field]).reset_index(drop=True)
    if pairs.empty:
        raise RuntimeError("tile/mukey join produced no non-empty mukey values")

    counts = pairs.groupby("tile", as_index=False)[mukey_field].count().rename(columns={mukey_field: "mukey_count"})
    counts = counts.sort_values(["tile"]).reset_index(drop=True)

    out_pairs = _resolve(output_tile_mukey_csv)
    out_counts = _resolve(output_tile_counts_csv)
    out_summary = _resolve(summary_json)
    out_pairs.parent.mkdir(parents=True, exist_ok=True)
    out_counts.parent.mkdir(parents=True, exist_ok=True)
    out_summary.parent.mkdir(parents=True, exist_ok=True)

    pairs.to_csv(out_pairs, index=False)
    counts.to_csv(out_counts, index=False)

    summary = {
        "inputs": {
            "filtered_csv": facts_path.as_posix(),
            "relative_path_field": relative_path_field,
            "tile_prefix_len": int(tile_prefix_len),
            "tiles_csv": str(tiles_csv or ""),
            "ssurgo_path": ssurgo_path_res.as_posix(),
            "ssurgo_layer": str(ssurgo_layer or ""),
            "ssurgo_layer_used": str(ssurgo_layer_used or ""),
            "mukey_field": str(mukey_field or ""),
            "states_csv": str(states_csv or ""),
            "state_field": str(state_field or ""),
            "state_filter_column_used": state_filter_column_used,
        },
        "counts": {
            "filtered_rows_used": int(len(tile_boxes)),
            "filtered_rows_skipped_bad_tile": int(skipped_bad_tile),
            "filtered_rows_skipped_not_selected_tile": int(skipped_not_selected),
            "filtered_rows_skipped_bad_bounds": int(skipped_bad_bounds),
            "tile_count": int(counts["tile"].nunique()),
            "tile_mukey_pairs": int(len(pairs)),
            "unique_mukey_count": int(pairs[mukey_field].nunique()),
            "ssurgo_rows_loaded": int(len(ssurgo)),
            "state_filtered_rows": int(state_filtered_rows),
        },
        "outputs": {
            "tile_mukey_csv": out_pairs.as_posix(),
            "tile_counts_csv": out_counts.as_posix(),
            "summary_json": out_summary.as_posix(),
        },
    }
    out_summary.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(description="Build tile->MUKEY mapping from filtered.csv rows and SSURGO polygons.")
    ap.add_argument("--filtered-csv", required=True, help="CSV with relative_path,minx,miny,maxx,maxy columns")
    ap.add_argument("--relative-path-field", default="relative_path")
    ap.add_argument("--minx-field", default="minx")
    ap.add_argument("--miny-field", default="miny")
    ap.add_argument("--maxx-field", default="maxx")
    ap.add_argument("--maxy-field", default="maxy")
    ap.add_argument("--tile-prefix-len", type=int, default=6)
    ap.add_argument("--tiles-csv", default="", help="Comma-separated tile ids; empty means all tiles in CSV")
    ap.add_argument("--ssurgo-path", required=True)
    ap.add_argument("--ssurgo-layer", default="MUPOLYGON")
    ap.add_argument("--mukey-field", default="mukey")
    ap.add_argument("--states-csv", default="")
    ap.add_argument("--state-field", default="areasymbol")
    ap.add_argument("--output-tile-mukey-csv", required=True)
    ap.add_argument("--output-tile-counts-csv", required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    summary = build_tile_mukey_map(
        filtered_csv=str(args.filtered_csv),
        relative_path_field=str(args.relative_path_field or "relative_path"),
        minx_field=str(args.minx_field or "minx"),
        miny_field=str(args.miny_field or "miny"),
        maxx_field=str(args.maxx_field or "maxx"),
        maxy_field=str(args.maxy_field or "maxy"),
        tile_prefix_len=int(args.tile_prefix_len or 6),
        tiles_csv=str(args.tiles_csv or ""),
        ssurgo_path=str(args.ssurgo_path),
        ssurgo_layer=str(args.ssurgo_layer or "MUPOLYGON"),
        mukey_field=str(args.mukey_field or "mukey"),
        states_csv=str(args.states_csv or ""),
        state_field=str(args.state_field or "areasymbol"),
        output_tile_mukey_csv=str(args.output_tile_mukey_csv),
        output_tile_counts_csv=str(args.output_tile_counts_csv),
        summary_json=str(args.summary_json),
        verbose=bool(args.verbose),
    )
    if args.verbose:
        print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
