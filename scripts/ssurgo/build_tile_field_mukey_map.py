#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
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


def _apply_state_filter(cand, *, states: list[str], state_field: str, verbose: bool):
    if not states:
        return cand, "", 0
    selected_col = ""
    if str(state_field or "").strip():
        selected_col = _lookup_col_case_insensitive(list(cand.columns), str(state_field))
    if not selected_col:
        for name in (
            "stusps",
            "state",
            "state_abbr",
            "state_name",
            "statefp",
            "areasymbol",
            "areasym",
            "area_symbol",
        ):
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


def _derive_tile_from_text(value: Any) -> str:
    text = _to_text(value).lower()
    m = TILE_RE.search(text)
    return m.group(1).lower() if m else ""


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
                print("[build_tile_field_mukey_map] derived tile_field_id from tile_id + field_id")
            return fields
        if has_field_id and has_source:
            fields = fields.copy()
            fields["tile_id"] = fields["source_name"].map(_derive_tile_from_text)
            fields[field_id_field] = (
                fields["tile_id"].map(_to_text).str.lower().str.strip() + "_" + fields["field_id"].map(_to_text).str.strip()
            )
            if verbose:
                print("[build_tile_field_mukey_map] derived tile_field_id from source_name + field_id")
            return fields
    cols = ", ".join(str(c) for c in fields.columns)
    raise ValueError(f"field_id_field not found/derivable: {field_id_field}; available columns: {cols}")


def _derive_tile_column(fields, tile_field: str, field_id_field: str, verbose: bool):
    selected = _lookup_col_case_insensitive(list(fields.columns), tile_field)
    if selected:
        fields = fields.copy()
        fields["tile"] = fields[selected].map(_derive_tile_from_text)
        fields = fields[fields["tile"].astype(str).str.len() > 0].copy()
        _vlog(verbose, f"derived tile from column={selected} rows={len(fields)}")
        return fields, selected

    fields = fields.copy()
    fields["tile"] = fields[field_id_field].map(lambda v: _derive_tile_from_text(_to_text(v).split("_")[0]))
    missing = int((fields["tile"].astype(str).str.len() == 0).sum())
    if missing > 0 and "source_name" in fields.columns:
        mask = fields["tile"].astype(str).str.len() == 0
        fields.loc[mask, "tile"] = fields.loc[mask, "source_name"].map(_derive_tile_from_text)
    fields = fields[fields["tile"].astype(str).str.len() > 0].copy()
    if fields.empty:
        raise RuntimeError("unable to derive tile ids for field polygons")
    _vlog(verbose, f"derived tile from {field_id_field}/source_name rows={len(fields)}")
    return fields, "<derived>"


def _load_fields(fields_path: str, fields_glob: str, field_id_field: str, tile_field: str, verbose: bool):
    try:
        import geopandas as gpd
        import pandas as pd
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("build_tile_field_mukey_map requires geopandas and pandas") from exc

    frames: list[Any] = []
    input_files: list[str] = []
    if str(fields_path or "").strip():
        p = _resolve(fields_path)
        if p.exists():
            gdf = gpd.read_file(p)
            gdf["source_name"] = p.name
            frames.append(gdf)
            input_files.append(p.as_posix())
        elif not str(fields_glob or "").strip():
            raise FileNotFoundError(f"fields_path not found: {p}")
        elif verbose:
            print(f"[build_tile_field_mukey_map][WARN] fields_path not found, falling back to fields_glob: {p}")

    if str(fields_glob or "").strip():
        matches = []
        for pat in _parse_glob_patterns(fields_glob):
            m = sorted(glob.glob(str(pat), recursive=True))
            if not m:
                m = sorted(glob.glob(str((_resolve(".") / str(pat)).as_posix()), recursive=True))
            matches.extend(m)
        for raw in matches:
            p = Path(raw)
            if not p.is_file():
                continue
            gdf = gpd.read_file(p)
            gdf["source_name"] = p.name
            frames.append(gdf)
            input_files.append(p.resolve().as_posix())

    if not frames:
        raise ValueError("provide fields_path or fields_glob with at least one vector file")

    fields = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), geometry="geometry")
    if fields.empty:
        raise RuntimeError("field polygons input is empty")
    if fields.crs is None:
        raise RuntimeError("field polygons missing CRS")
    fields = _derive_field_id_column(fields, field_id_field, verbose)
    fields, tile_source = _derive_tile_column(fields, tile_field, field_id_field, verbose)
    fields[field_id_field] = fields[field_id_field].map(_to_text)
    fields = fields[fields[field_id_field].astype(str).str.len() > 0].copy()
    fields = fields[fields.geometry.notna() & ~fields.geometry.is_empty].copy()
    if fields.empty:
        raise RuntimeError("field polygons empty after id/tile/geometry filtering")
    _vlog(verbose, f"loaded field polygons rows={len(fields)} files={len(input_files)} tile_source={tile_source}")
    return fields, input_files, tile_source


def _list_vector_layers(path: Path) -> list[str]:
    try:
        import fiona

        layers = fiona.listlayers(path.as_posix())
        if layers:
            return [str(x) for x in layers if str(x).strip()]
    except Exception:
        pass
    try:
        import pyogrio

        layers_info = pyogrio.list_layers(path.as_posix())
        out: list[str] = []
        for row in layers_info:  # type: ignore[assignment]
            try:
                name = str(row[0]).strip()
            except Exception:
                name = str(row).strip()
            if name:
                out.append(name)
        return out
    except Exception:
        return []


def _resolve_layer_crs(path: Path, layer_name: str):
    try:
        import fiona

        if layer_name:
            with fiona.open(path.as_posix(), layer=layer_name) as src:
                return src.crs_wkt or src.crs
        with fiona.open(path.as_posix()) as src:
            return src.crs_wkt or src.crs
    except Exception:
        return None


def _transform_bbox(bounds: tuple[float, float, float, float], src_crs: Any, dst_crs: Any):
    if src_crs is None or dst_crs is None:
        return bounds
    try:
        from pyproj import CRS, Transformer

        src = CRS.from_user_input(src_crs)
        dst = CRS.from_user_input(dst_crs)
        if src == dst:
            return bounds
        minx, miny, maxx, maxy = bounds
        xs = [minx, minx, maxx, maxx]
        ys = [miny, maxy, miny, maxy]
        transformer = Transformer.from_crs(src, dst, always_xy=True)
        tx, ty = transformer.transform(xs, ys)
        return (float(min(tx)), float(min(ty)), float(max(tx)), float(max(ty)))
    except Exception:
        return bounds


def _load_ssurgo_polygons(
    *,
    ssurgo_path: str,
    ssurgo_glob: str,
    ssurgo_layer: str,
    ssurgo_where: str,
    states_csv: str,
    state_field: str,
    mukey_field: str,
    bbox: tuple[float, float, float, float],
    bbox_crs: Any,
    verbose: bool,
):
    try:
        import geopandas as gpd
        import pandas as pd
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("build_tile_field_mukey_map requires geopandas and pandas") from exc

    ssurgo_files: list[Path] = []
    ssurgo_path_resolved = _resolve(ssurgo_path) if str(ssurgo_path or "").strip() else None
    if ssurgo_path_resolved is not None:
        if ssurgo_path_resolved.exists():
            ssurgo_files.append(ssurgo_path_resolved)
        elif not str(ssurgo_glob or "").strip():
            raise FileNotFoundError(f"ssurgo_path not found: {ssurgo_path_resolved}")
        elif verbose:
            print(f"[build_tile_field_mukey_map][WARN] ssurgo_path not found, falling back to ssurgo_glob: {ssurgo_path_resolved}")
    if str(ssurgo_glob or "").strip():
        matches_all: list[str] = []
        for pat in _parse_glob_patterns(ssurgo_glob):
            m = sorted(glob.glob(str(pat), recursive=True))
            if not m:
                m = sorted(glob.glob(str((_resolve(".") / str(pat)).as_posix()), recursive=True))
            matches_all.extend(m)
        seen_match: set[str] = set()
        for raw in matches_all:
            p = Path(raw)
            key = p.resolve().as_posix().lower()
            if key in seen_match:
                continue
            seen_match.add(key)
            if p.is_file() or (p.is_dir() and p.suffix.lower() in {".gdb"}):
                if p not in ssurgo_files:
                    ssurgo_files.append(p)
    if not ssurgo_files:
        raise FileNotFoundError("no SSURGO files found via ssurgo_path/ssurgo_glob")

    states = _parse_states_csv(states_csv)
    frames: list[Any] = []
    loaded_files: list[str] = []
    total_state_filtered_rows = 0
    state_filter_column_used = ""
    for p in ssurgo_files:
        layer_candidates: list[str] = []
        if str(ssurgo_layer or "").strip():
            layer_candidates.append(str(ssurgo_layer).strip())
        layer_candidates.extend(["MUPOLYGON", "mupolygon", "MapunitPoly", "mapunit", "ssurgo_mapunit"])
        if p.suffix.lower() in {".gdb", ".gpkg"}:
            layer_candidates.extend(_list_vector_layers(p))
        layer_candidates.append("")

        deduped: list[str] = []
        seen_keys: set[str] = set()
        for item in layer_candidates:
            key = str(item).strip().lower()
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(str(item))

        loaded = False
        for layer_name in deduped:
            read_kwargs: dict[str, Any] = {}
            if layer_name:
                read_kwargs["layer"] = layer_name
            if str(ssurgo_where or "").strip():
                read_kwargs["where"] = str(ssurgo_where).strip()
            layer_crs = _resolve_layer_crs(p, str(layer_name or ""))
            read_kwargs["bbox"] = _transform_bbox(bbox, bbox_crs, layer_crs)
            try:
                cand = gpd.read_file(p, **read_kwargs)
            except Exception:
                continue
            if cand is None or cand.empty:
                continue
            actual_mukey_col = _lookup_col_case_insensitive(list(cand.columns), mukey_field)
            if not actual_mukey_col:
                continue
            if cand.crs is None:
                continue
            if states:
                cand, state_col_used, state_dropped = _apply_state_filter(
                    cand,
                    states=states,
                    state_field=state_field,
                    verbose=verbose,
                )
                if state_col_used:
                    state_filter_column_used = state_col_used
                total_state_filtered_rows += int(state_dropped)
                if cand.empty:
                    continue
            cand = cand[[actual_mukey_col, "geometry"]].copy()
            if actual_mukey_col != mukey_field:
                cand = cand.rename(columns={actual_mukey_col: mukey_field})
            cand[mukey_field] = cand[mukey_field].map(_to_text)
            cand = cand[cand[mukey_field].astype(str).str.len() > 0].copy()
            cand = cand[cand.geometry.notna() & ~cand.geometry.is_empty].copy()
            if cand.empty:
                continue
            frames.append(cand)
            loaded_files.append(p.as_posix())
            loaded = True
            _vlog(verbose, f"loaded ssurgo file={p.as_posix()} layer={layer_name or '<default>'} rows={len(cand)}")
            break
        if not loaded:
            _vlog(verbose, f"skipping ssurgo file with no usable layer: {p.as_posix()}")

    if not frames:
        raise RuntimeError("no usable SSURGO polygon layers loaded")

    ssurgo = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), geometry="geometry")
    if ssurgo.crs is None:
        ssurgo = gpd.GeoDataFrame(ssurgo, geometry="geometry", crs=frames[0].crs)
    return ssurgo, loaded_files, state_filter_column_used, total_state_filtered_rows


def _load_tile_mukey_pairs(tile_mukey_csv: str, tile_field: str, mukey_field: str):
    path = _resolve(tile_mukey_csv)
    if not path.exists():
        raise FileNotFoundError(f"tile_mukey_csv not found: {path.as_posix()}")
    pairs: set[tuple[str, str]] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            raise ValueError("tile_mukey_csv has no header")
        tile_col = _lookup_col_case_insensitive(list(rdr.fieldnames), tile_field)
        mukey_col = _lookup_col_case_insensitive(list(rdr.fieldnames), mukey_field)
        if not tile_col or not mukey_col:
            raise ValueError(f"tile_mukey_csv must include columns {tile_field} and {mukey_field}")
        for row in rdr:
            tile = _derive_tile_from_text((row or {}).get(tile_col))
            mukey = _to_text((row or {}).get(mukey_col)).strip()
            if not tile or not mukey:
                continue
            pairs.add((tile, mukey))
    if not pairs:
        raise RuntimeError("tile_mukey_csv has no usable tile,mukey rows")
    return path, pairs


def build_tile_field_mukey_map(
    *,
    tile_mukey_csv: str,
    fields_path: str,
    fields_glob: str,
    ssurgo_path: str,
    ssurgo_glob: str,
    ssurgo_layer: str,
    ssurgo_where: str,
    states_csv: str,
    state_field: str,
    field_id_field: str,
    tile_field: str,
    mukey_field: str,
    target_crs: str,
    output_csv: str,
    output_long_csv: str,
    summary_json: str,
    verbose: bool,
) -> dict[str, Any]:
    try:
        import geopandas as gpd
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("build_tile_field_mukey_map requires geopandas") from exc

    tile_mukey_path, allowed_tile_mukey_pairs = _load_tile_mukey_pairs(tile_mukey_csv, tile_field, mukey_field)
    _vlog(verbose, f"loaded tile_mukey pairs={len(allowed_tile_mukey_pairs)}")

    fields, input_files, tile_source = _load_fields(fields_path, fields_glob, field_id_field, tile_field, verbose)
    if target_crs.strip():
        fields = fields.to_crs(target_crs)
    fields_bbox = tuple(float(v) for v in fields.total_bounds.tolist())
    fields_bbox_crs: Any = fields.crs
    _vlog(verbose, f"field rows={len(fields)} unique_tiles={fields['tile'].nunique()}")

    ssurgo, ssurgo_loaded_files, state_filter_column_used, state_filtered_rows = _load_ssurgo_polygons(
        ssurgo_path=ssurgo_path,
        ssurgo_glob=ssurgo_glob,
        ssurgo_layer=ssurgo_layer,
        ssurgo_where=ssurgo_where,
        states_csv=states_csv,
        state_field=state_field,
        mukey_field=mukey_field,
        bbox=fields_bbox,
        bbox_crs=fields_bbox_crs,
        verbose=verbose,
    )

    if target_crs.strip():
        ssurgo = ssurgo.to_crs(target_crs)
    else:
        fields = fields.to_crs(ssurgo.crs)

    field_src = fields[["tile", field_id_field, "source_name", "geometry"]].copy()
    field_src["field_area"] = field_src.geometry.area
    field_src = field_src[field_src["field_area"] > 0].copy()
    if field_src.empty:
        raise RuntimeError("field polygons have zero area after projection")

    ssurgo_src = ssurgo[[mukey_field, "geometry"]].copy()
    ssurgo_src[mukey_field] = ssurgo_src[mukey_field].map(_to_text)
    ssurgo_src = ssurgo_src[ssurgo_src[mukey_field].astype(str).str.len() > 0].copy()
    if ssurgo_src.empty:
        raise RuntimeError("SSURGO polygons empty after mukey filtering")

    _vlog(verbose, "running field x ssurgo intersection overlay")
    intersections = gpd.overlay(field_src, ssurgo_src, how="intersection")
    if intersections.empty:
        raise RuntimeError("no field polygons intersect SSURGO polygons")
    intersections["overlap_area"] = intersections.geometry.area
    intersections = intersections[intersections["overlap_area"] > 0].copy()
    if intersections.empty:
        raise RuntimeError("overlay produced no positive-area intersections")

    grouped_pairs = (
        intersections.groupby(["tile", field_id_field, "source_name", "field_area", mukey_field], as_index=False)["overlap_area"].sum()
    )

    filtered_rows: list[dict[str, Any]] = []
    dropped_not_in_tile_mukey = 0
    for row in grouped_pairs.itertuples(index=False):
        tile = _derive_tile_from_text(getattr(row, "tile", ""))
        field_id = _to_text(getattr(row, field_id_field))
        mukey = _to_text(getattr(row, mukey_field)).strip()
        field_area = float(getattr(row, "field_area") or 0.0)
        overlap_area = float(getattr(row, "overlap_area") or 0.0)
        if not tile or not field_id or not mukey:
            continue
        if (tile, mukey) not in allowed_tile_mukey_pairs:
            dropped_not_in_tile_mukey += 1
            continue
        pct_field_overlap = (overlap_area / field_area * 100.0) if field_area > 0 else 0.0
        filtered_rows.append(
            {
                "tile": tile,
                field_id_field: field_id,
                mukey_field: mukey,
                "source_name": _to_text(getattr(row, "source_name", "")),
                "overlap_area": overlap_area,
                "field_area": field_area,
                "pct_field_overlap": pct_field_overlap,
            }
        )
    if not filtered_rows:
        raise RuntimeError("no tile/field/mukey rows remained after tile_mukey filtering")
    filtered_rows.sort(key=lambda r: (str(r["tile"]), str(r[field_id_field]), str(r[mukey_field])))

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in filtered_rows:
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

    output_csv_path = _resolve(output_csv)
    output_long_path = _resolve(output_long_csv)
    summary_path = _resolve(summary_json)
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    output_long_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    with output_long_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["tile", field_id_field, mukey_field, "source_name", "overlap_area", "field_area", "pct_field_overlap"],
        )
        w.writeheader()
        for row in filtered_rows:
            w.writerow(row)

    with output_csv_path.open("w", encoding="utf-8", newline="") as f:
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

    summary = {
        "inputs": {
            "tile_mukey_csv": tile_mukey_path.as_posix(),
            "fields_path": str(fields_path or ""),
            "fields_glob": str(fields_glob or ""),
            "ssurgo_path": str(ssurgo_path or ""),
            "ssurgo_glob": str(ssurgo_glob or ""),
            "ssurgo_layer": str(ssurgo_layer or ""),
            "ssurgo_where": str(ssurgo_where or ""),
            "states_csv": str(states_csv or ""),
            "state_field": str(state_field or ""),
            "state_filter_column_used": state_filter_column_used,
            "field_id_field": field_id_field,
            "tile_field": tile_field,
            "mukey_field": mukey_field,
            "target_crs": target_crs or str(ssurgo.crs),
            "tile_source": tile_source,
            "input_files": input_files,
            "ssurgo_loaded_files": ssurgo_loaded_files,
        },
        "counts": {
            "tile_mukey_pairs": int(len(allowed_tile_mukey_pairs)),
            "field_rows": int(len(field_src)),
            "ssurgo_rows": int(len(ssurgo_src)),
            "intersection_rows": int(len(intersections)),
            "candidate_pairs_before_tile_mukey_filter": int(len(grouped_pairs)),
            "pairs_dropped_not_in_tile_mukey": int(dropped_not_in_tile_mukey),
            "unique_tile_field_mukey_pairs": int(len(filtered_rows)),
            "unique_tile_field_rows": int(len(grouped)),
            "state_filtered_rows": int(state_filtered_rows),
        },
        "outputs": {
            "output_csv": output_csv_path.as_posix(),
            "output_long_csv": output_long_path.as_posix(),
            "summary_json": summary_path.as_posix(),
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    _vlog(verbose, f"complete rows={summary['counts']['unique_tile_field_mukey_pairs']}")
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build tile->field->mukey map with percent field overlap, filtered by existing tile->mukey pairs."
    )
    ap.add_argument("--tile-mukey-csv", required=True, help="CSV produced by build_tile_mukey_map.py")
    ap.add_argument("--fields-path", default="", help="Single field polygons vector path")
    ap.add_argument("--fields-glob", default="", help="Glob for field polygon vectors")
    ap.add_argument("--ssurgo-path", required=True, help="SSURGO polygon layer path")
    ap.add_argument("--ssurgo-glob", default="", help="Glob for SSURGO polygon files")
    ap.add_argument("--ssurgo-layer", default="", help="Optional SSURGO layer name")
    ap.add_argument("--ssurgo-where", default="", help="Optional OGR SQL where-clause for SSURGO reads")
    ap.add_argument("--states-csv", default="", help="Optional state filter list")
    ap.add_argument("--state-field", default="areasymbol", help="Optional SSURGO state-like field")
    ap.add_argument("--field-id-field", default="tile_field_id")
    ap.add_argument("--tile-field", default="tile_id", help="Field polygon column containing tile id")
    ap.add_argument("--mukey-field", default="mukey")
    ap.add_argument("--target-crs", default="EPSG:5070")
    ap.add_argument("--output-csv", required=True, help="Wide CSV grouped by tile+field")
    ap.add_argument("--output-long-csv", required=True, help="Long CSV one row per tile+field+mukey")
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    build_tile_field_mukey_map(
        tile_mukey_csv=str(args.tile_mukey_csv),
        fields_path=str(args.fields_path or ""),
        fields_glob=str(args.fields_glob or ""),
        ssurgo_path=str(args.ssurgo_path),
        ssurgo_glob=str(args.ssurgo_glob or ""),
        ssurgo_layer=str(args.ssurgo_layer or ""),
        ssurgo_where=str(args.ssurgo_where or ""),
        states_csv=str(args.states_csv or ""),
        state_field=str(args.state_field or ""),
        field_id_field=str(args.field_id_field),
        tile_field=str(args.tile_field),
        mukey_field=str(args.mukey_field),
        target_crs=str(args.target_crs or ""),
        output_csv=str(args.output_csv),
        output_long_csv=str(args.output_long_csv),
        summary_json=str(args.summary_json),
        verbose=bool(args.verbose),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
