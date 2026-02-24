#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import re
from pathlib import Path
from typing import Any


TILE_RE = re.compile(r"(h\d{2}v\d{2})", re.IGNORECASE)


def _resolve(path_text: str) -> Path:
    return Path(str(path_text or "")).expanduser().resolve()


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:  # noqa: BLE001
            pass
    return str(value)


def _vlog(verbose: bool, message: str) -> None:
    if verbose:
        print(f"[build_tile_field_mukey_map] {message}")


def _parse_glob_patterns(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for token in str(raw or "").replace(";", ",").split(","):
        pat = str(token or "").strip()
        if not pat:
            continue
        if pat in seen:
            continue
        seen.add(pat)
        out.append(pat)
    return out


def _lookup_col_case_insensitive(cols: list[Any], target: str) -> str:
    by_lower = {str(c).lower(): str(c) for c in cols}
    return str(by_lower.get(str(target or "").strip().lower()) or "")


def _derive_tile_from_text(value: Any) -> str:
    text = _to_text(value).lower()
    m = TILE_RE.search(text)
    return m.group(1).lower() if m else ""


def _normalize_tile(tile_text: str) -> str:
    t = _derive_tile_from_text(tile_text)
    if not t:
        raise ValueError(f"invalid tile id: {tile_text}")
    return t


def _derive_field_id_column(fields, field_id_field: str, verbose: bool):
    if field_id_field in fields.columns:
        return fields

    if str(field_id_field).strip().lower() == "tile_field_id":
        has_field_id = "field_id" in fields.columns
        has_tile_id = "tile_id" in fields.columns
        has_source = "source_name" in fields.columns
        if has_field_id and has_tile_id:
            fields = fields.copy()
            fields[field_id_field] = (
                fields["tile_id"].map(_to_text).str.lower().str.strip() + "_" + fields["field_id"].map(_to_text).str.strip()
            )
            if verbose:
                _vlog(verbose, "derived tile_field_id from tile_id + field_id")
            return fields
        if has_field_id and has_source:
            fields = fields.copy()
            fields["tile_id"] = fields["source_name"].map(_derive_tile_from_text)
            fields[field_id_field] = (
                fields["tile_id"].map(_to_text).str.lower().str.strip() + "_" + fields["field_id"].map(_to_text).str.strip()
            )
            if verbose:
                _vlog(verbose, "derived tile_field_id from source_name + field_id")
            return fields

    cols = ", ".join(str(c) for c in fields.columns)
    raise ValueError(f"field_id_field not found/derivable: {field_id_field}; available columns: {cols}")


def _load_tile_mukeys(tile_mukey_csv: str, tile: str, tile_field: str, mukey_field: str) -> tuple[Path, set[int], str, str]:
    path = _resolve(tile_mukey_csv)
    if not path.exists():
        raise FileNotFoundError(f"tile_mukey_csv not found: {path.as_posix()}")

    out: set[int] = set()
    tile_col_used = ""
    mukey_col_used = ""
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            raise ValueError("tile_mukey_csv has no header")
        cols = list(rdr.fieldnames)
        tile_col = _lookup_col_case_insensitive(cols, tile_field)
        if not tile_col:
            for alias in ("tile", "tile_id"):
                tile_col = _lookup_col_case_insensitive(cols, alias)
                if tile_col:
                    break
        mukey_col = _lookup_col_case_insensitive(cols, mukey_field)
        if not tile_col or not mukey_col:
            raise ValueError(
                f"tile_mukey_csv must include tile ({tile_field} or tile/tile_id) and {mukey_field}; available={cols}"
            )
        tile_col_used = tile_col
        mukey_col_used = mukey_col

        wanted = _normalize_tile(tile)
        for row in rdr:
            row_tile = _derive_tile_from_text((row or {}).get(tile_col))
            if row_tile != wanted:
                continue
            mk_text = _to_text((row or {}).get(mukey_col)).strip()
            if not mk_text:
                continue
            try:
                mk = int(float(mk_text))
            except Exception:
                continue
            if mk > 0:
                out.add(mk)

    if not out:
        raise RuntimeError(f"no mukeys found in tile_mukey_csv for tile={tile}")
    return path, out, tile_col_used, mukey_col_used


def _expand_fields_files(fields_path: str, fields_glob: str) -> list[Path]:
    files: list[Path] = []
    seen: set[str] = set()
    if str(fields_path or "").strip():
        p = _resolve(fields_path)
        if p.exists() and p.is_file():
            key = p.resolve().as_posix().lower()
            seen.add(key)
            files.append(p.resolve())
        elif not str(fields_glob or "").strip():
            raise FileNotFoundError(f"fields_path not found: {p}")

    for pat in _parse_glob_patterns(fields_glob):
        matches = sorted(glob.glob(str(pat), recursive=True))
        if not matches:
            matches = sorted(glob.glob(str((_resolve(".") / str(pat)).as_posix()), recursive=True))
        for raw in matches:
            p = Path(raw)
            if not p.is_file():
                continue
            key = p.resolve().as_posix().lower()
            if key in seen:
                continue
            seen.add(key)
            files.append(p.resolve())
    return files


def _load_fields_for_tile(
    *,
    tile: str,
    fields_path: str,
    fields_glob: str,
    field_id_field: str,
    tile_field: str,
    verbose: bool,
):
    try:
        import geopandas as gpd
        import pandas as pd
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("build_tile_field_mukey_map requires geopandas and pandas") from exc

    tile_norm = _normalize_tile(tile)
    files = _expand_fields_files(fields_path, fields_glob)
    if not files:
        raise ValueError("provide fields_path or fields_glob with at least one vector file")

    tile_hint = tile_norm.lower()
    selected_files: list[Path] = [p for p in files if tile_hint in p.name.lower()]
    if not selected_files:
        # Fallback if filenames do not include tile token.
        selected_files = files

    frames: list[Any] = []
    input_files: list[str] = []
    for p in selected_files:
        gdf = gpd.read_file(p)
        if gdf is None or gdf.empty:
            continue
        gdf["source_name"] = p.name
        frames.append(gdf)
        input_files.append(p.as_posix())

    if not frames:
        raise RuntimeError(f"no field polygons loaded for tile={tile_norm}")

    fields = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), geometry="geometry")
    if fields.empty:
        raise RuntimeError(f"field polygons empty for tile={tile_norm}")
    if fields.crs is None:
        raise RuntimeError("field polygons missing CRS")

    fields = _derive_field_id_column(fields, field_id_field, verbose)

    tile_col = _lookup_col_case_insensitive(list(fields.columns), tile_field)
    if tile_col:
        fields = fields.copy()
        fields["tile"] = fields[tile_col].map(_derive_tile_from_text)
    else:
        fields = fields.copy()
        fields["tile"] = fields[field_id_field].map(lambda v: _derive_tile_from_text(_to_text(v).split("_")[0]))
        if "source_name" in fields.columns:
            miss = fields["tile"].astype(str).str.len() == 0
            fields.loc[miss, "tile"] = fields.loc[miss, "source_name"].map(_derive_tile_from_text)

    fields[field_id_field] = fields[field_id_field].map(_to_text)
    fields = fields[fields[field_id_field].astype(str).str.len() > 0].copy()
    fields = fields[fields.geometry.notna() & ~fields.geometry.is_empty].copy()
    fields = fields[fields["tile"].map(_derive_tile_from_text) == tile_norm].copy()
    if fields.empty:
        raise RuntimeError(f"no field polygons found for tile={tile_norm}")

    return fields, input_files


def _tile_field_mukey_from_raster(
    *,
    fields,
    tile: str,
    mukeys: set[int],
    ssurgo_raster_path: str,
    field_id_field: str,
    mukey_field: str,
    verbose: bool,
):
    import numpy as np
    import rasterio
    from rasterio.features import rasterize
    from rasterio.windows import from_bounds

    raster_path = _resolve(ssurgo_raster_path)
    if not raster_path.exists():
        raise FileNotFoundError(f"ssurgo raster path not found: {raster_path.as_posix()}")

    with rasterio.open(raster_path) as ds:
        if ds.crs is None:
            raise RuntimeError("SSURGO raster missing CRS")

        if fields.crs != ds.crs:
            fields = fields.to_crs(ds.crs)

        bounds = tuple(float(v) for v in fields.total_bounds.tolist())
        minx, miny, maxx, maxy = bounds
        if maxx <= minx or maxy <= miny:
            raise RuntimeError(f"invalid tile bounds for tile={tile}")

        win = from_bounds(minx, miny, maxx, maxy, transform=ds.transform).round_offsets().round_lengths()
        if win.width <= 0 or win.height <= 0:
            raise RuntimeError(f"empty raster window for tile={tile}")

        mukey_arr = ds.read(1, window=win, masked=False)
        if mukey_arr.size == 0:
            raise RuntimeError(f"empty raster array for tile={tile}")

        win_transform = ds.window_transform(win)
        nodata = ds.nodata

        field_rows = fields.reset_index(drop=True).copy()
        field_rows["_fid_idx"] = np.arange(1, len(field_rows) + 1, dtype=np.int64)
        field_rows["field_area"] = field_rows.geometry.area
        field_rows = field_rows[field_rows["field_area"] > 0].copy()
        if field_rows.empty:
            raise RuntimeError(f"no positive-area field polygons for tile={tile}")

        shapes = [(geom, int(fid)) for geom, fid in zip(field_rows.geometry, field_rows["_fid_idx"], strict=False)]
        field_idx_arr = rasterize(
            shapes,
            out_shape=(int(win.height), int(win.width)),
            transform=win_transform,
            fill=0,
            all_touched=False,
            dtype="int32",
        )

        valid = field_idx_arr > 0
        if nodata is not None:
            valid &= mukey_arr != nodata
        valid &= mukey_arr > 0

        if mukeys:
            valid &= np.isin(mukey_arr, np.array(sorted(mukeys), dtype=mukey_arr.dtype))

        if not np.any(valid):
            raise RuntimeError(f"no overlapping raster cells for tile={tile} after mukey filter")

        fid_vals = field_idx_arr[valid].astype(np.int64)
        mukey_vals = mukey_arr[valid].astype(np.int64)
        pairs = np.column_stack((fid_vals, mukey_vals))
        uniq, counts = np.unique(pairs, axis=0, return_counts=True)

        # Pixel area in CRS units (for EPSG:5070 this is m^2).
        a = float(win_transform.a)
        b = float(win_transform.b)
        d = float(win_transform.d)
        e = float(win_transform.e)
        pixel_area = abs(a * e - b * d)
        if pixel_area <= 0:
            raise RuntimeError("invalid raster transform pixel area")

        field_meta: dict[int, dict[str, Any]] = {}
        for row in field_rows.itertuples(index=False):
            idx = int(getattr(row, "_fid_idx"))
            field_meta[idx] = {
                "field_id": _to_text(getattr(row, field_id_field)),
                "source_name": _to_text(getattr(row, "source_name", "")),
                "field_area": float(getattr(row, "field_area") or 0.0),
            }

        long_rows: list[dict[str, Any]] = []
        for i in range(len(uniq)):
            fid = int(uniq[i, 0])
            mk = int(uniq[i, 1])
            cnt = int(counts[i])
            meta = field_meta.get(fid)
            if not meta:
                continue
            field_area = float(meta["field_area"] or 0.0)
            overlap_area = float(cnt) * pixel_area
            pct_field_overlap = (overlap_area / field_area * 100.0) if field_area > 0 else 0.0
            long_rows.append(
                {
                    "tile": _normalize_tile(tile),
                    field_id_field: str(meta["field_id"]),
                    mukey_field: str(mk),
                    "source_name": str(meta["source_name"]),
                    "overlap_area": overlap_area,
                    "field_area": field_area,
                    "pct_field_overlap": pct_field_overlap,
                    "pixel_count": cnt,
                    "pixel_area": pixel_area,
                }
            )

    if not long_rows:
        raise RuntimeError(f"no tile/field/mukey rows produced for tile={tile}")

    long_rows.sort(key=lambda r: (str(r["tile"]), str(r[field_id_field]), str(r[mukey_field])))
    return long_rows, fields, raster_path


def _write_outputs(
    *,
    long_rows: list[dict[str, Any]],
    field_id_field: str,
    mukey_field: str,
    output_csv: str,
    output_long_csv: str,
):
    out_csv = _resolve(output_csv)
    out_long = _resolve(output_long_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_long.parent.mkdir(parents=True, exist_ok=True)

    with out_long.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["tile", field_id_field, mukey_field, "source_name", "overlap_area", "field_area", "pct_field_overlap", "pixel_count", "pixel_area"],
        )
        w.writeheader()
        for row in long_rows:
            w.writerow(row)

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in long_rows:
        key = (str(row["tile"]), str(row[field_id_field]))
        item = grouped.setdefault(
            key,
            {
                "tile": str(row["tile"]),
                field_id_field: str(row[field_id_field]),
                "source_name": str(row.get("source_name") or ""),
                "field_area": float(row.get("field_area") or 0.0),
                "mukeys": [],
                "mukey_pct": {},
                "mukey_overlap_area": {},
            },
        )
        mk = str(row[mukey_field])
        if mk not in item["mukeys"]:
            item["mukeys"].append(mk)
        item["mukey_pct"][mk] = float(row.get("pct_field_overlap") or 0.0)
        item["mukey_overlap_area"][mk] = float(row.get("overlap_area") or 0.0)

    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "tile",
                field_id_field,
                "mukey_count",
                "mukeys_json",
                "mukey_pct_field_json",
                "mukey_overlap_area_json",
                "field_area",
                "source_name",
            ],
        )
        w.writeheader()
        for key in sorted(grouped.keys()):
            item = grouped[key]
            mukeys = sorted(item["mukeys"])
            pct_map = {mk: item["mukey_pct"].get(mk, 0.0) for mk in mukeys}
            overlap_map = {mk: item["mukey_overlap_area"].get(mk, 0.0) for mk in mukeys}
            w.writerow(
                {
                    "tile": item["tile"],
                    field_id_field: item[field_id_field],
                    "mukey_count": len(mukeys),
                    "mukeys_json": json.dumps(mukeys, separators=(",", ":")),
                    "mukey_pct_field_json": json.dumps(pct_map, separators=(",", ":")),
                    "mukey_overlap_area_json": json.dumps(overlap_map, separators=(",", ":")),
                    "field_area": item.get("field_area", 0.0),
                    "source_name": item.get("source_name", ""),
                }
            )

    return out_csv, out_long, grouped


def build_tile_field_mukey_map(
    *,
    tile: str,
    tile_mukey_csv: str,
    fields_path: str,
    fields_glob: str,
    ssurgo_path: str,
    field_id_field: str,
    tile_field: str,
    mukey_field: str,
    output_csv: str,
    output_long_csv: str,
    summary_json: str,
    verbose: bool,
) -> dict[str, Any]:
    tile_norm = _normalize_tile(tile)
    tile_mukey_path, tile_mukeys, tile_col_used, mukey_col_used = _load_tile_mukeys(
        tile_mukey_csv,
        tile_norm,
        tile_field,
        mukey_field,
    )
    _vlog(verbose, f"tile={tile_norm} mukeys={len(tile_mukeys)} from={tile_mukey_path.as_posix()}")

    fields, input_files = _load_fields_for_tile(
        tile=tile_norm,
        fields_path=fields_path,
        fields_glob=fields_glob,
        field_id_field=field_id_field,
        tile_field=tile_field,
        verbose=verbose,
    )

    long_rows, fields_projected, raster_path = _tile_field_mukey_from_raster(
        fields=fields,
        tile=tile_norm,
        mukeys=tile_mukeys,
        ssurgo_raster_path=ssurgo_path,
        field_id_field=field_id_field,
        mukey_field=mukey_field,
        verbose=verbose,
    )

    out_csv, out_long, grouped = _write_outputs(
        long_rows=long_rows,
        field_id_field=field_id_field,
        mukey_field=mukey_field,
        output_csv=output_csv,
        output_long_csv=output_long_csv,
    )

    summary_path = _resolve(summary_json)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "inputs": {
            "tile": tile_norm,
            "tile_mukey_csv": tile_mukey_path.as_posix(),
            "tile_mukey_tile_col_used": tile_col_used,
            "tile_mukey_mukey_col_used": mukey_col_used,
            "fields_path": str(fields_path or ""),
            "fields_glob": str(fields_glob or ""),
            "ssurgo_path": raster_path.as_posix(),
            "field_id_field": field_id_field,
            "tile_field": tile_field,
            "mukey_field": mukey_field,
            "input_files": input_files,
        },
        "counts": {
            "tile_mukey_count": int(len(tile_mukeys)),
            "field_rows": int(len(fields_projected)),
            "unique_tile_field_mukey_pairs": int(len(long_rows)),
            "unique_tile_field_rows": int(len(grouped)),
        },
        "outputs": {
            "output_csv": out_csv.as_posix(),
            "output_long_csv": out_long.as_posix(),
            "summary_json": summary_path.as_posix(),
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    _vlog(verbose, f"complete tile={tile_norm} pairs={len(long_rows)}")
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build tile->field->mukey map for one tile using SSURGO MUKEY raster intersections."
    )
    ap.add_argument("--tile", required=True, help="Tile id (e.g. h17v08)")
    ap.add_argument("--tile-mukey-csv", required=True, help="CSV produced by build_tile_mukey_map.py")
    ap.add_argument("--fields-path", default="", help="Single field polygons vector path")
    ap.add_argument("--fields-glob", default="", help="Glob for field polygon vectors")
    ap.add_argument("--ssurgo-path", required=True, help="SSURGO MUKEY raster path (e.g. MURASTER_30m.tif)")
    ap.add_argument("--field-id-field", default="tile_field_id")
    ap.add_argument("--tile-field", default="tile_id", help="Field polygon column containing tile id")
    ap.add_argument("--mukey-field", default="mukey")
    ap.add_argument("--output-csv", required=True, help="Wide CSV grouped by tile+field")
    ap.add_argument("--output-long-csv", required=True, help="Long CSV one row per tile+field+mukey")
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    build_tile_field_mukey_map(
        tile=str(args.tile),
        tile_mukey_csv=str(args.tile_mukey_csv),
        fields_path=str(args.fields_path or ""),
        fields_glob=str(args.fields_glob or ""),
        ssurgo_path=str(args.ssurgo_path),
        field_id_field=str(args.field_id_field),
        tile_field=str(args.tile_field),
        mukey_field=str(args.mukey_field),
        output_csv=str(args.output_csv),
        output_long_csv=str(args.output_long_csv),
        summary_json=str(args.summary_json),
        verbose=bool(args.verbose),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
