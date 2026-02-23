#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
import re
from pathlib import Path
from typing import Any


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
        print(f"[build_field_mukey_map] {message}")


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


_TILE_ID_RE = re.compile(r"(h\d{2}v\d{2})", re.IGNORECASE)


def _derive_field_id_column(fields, field_id_field: str, verbose: bool):
    # Fast-path: requested field id field already exists.
    if field_id_field in fields.columns:
        return fields

    # Common requested key: tile_field_id.
    if str(field_id_field).strip().lower() == "tile_field_id":
        has_field_id = "field_id" in fields.columns
        has_tile_id = "tile_id" in fields.columns
        has_source = "source_name" in fields.columns
        if has_field_id and has_tile_id:
            fields = fields.copy()
            fields[field_id_field] = (
                fields["tile_id"].map(_to_text).str.lower().str.strip()
                + "_"
                + fields["field_id"].map(_to_text).str.strip()
            )
            if verbose:
                print("[build_field_mukey_map] derived tile_field_id from tile_id + field_id")
            return fields

        if has_field_id and has_source:
            fields = fields.copy()

            def _tile_from_source(v: Any) -> str:
                text = _to_text(v).lower()
                m = _TILE_ID_RE.search(text)
                return m.group(1).lower() if m else ""

            fields["tile_id"] = fields["source_name"].map(_tile_from_source)
            fields[field_id_field] = (
                fields["tile_id"].map(_to_text).str.lower().str.strip()
                + "_"
                + fields["field_id"].map(_to_text).str.strip()
            )
            if verbose:
                print("[build_field_mukey_map] derived tile_field_id from source_name + field_id")
            return fields

    cols = ", ".join(str(c) for c in fields.columns)
    raise ValueError(f"field_id_field not found/derivable: {field_id_field}; available columns: {cols}")


def _load_fields(fields_path: str, fields_glob: str, field_id_field: str, verbose: bool):
    try:
        import geopandas as gpd
        import pandas as pd
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("build_field_mukey_map requires geopandas and pandas") from exc

    frames: list[Any] = []
    input_files: list[str] = []
    _vlog(verbose, f"loading field polygons fields_path='{fields_path}' fields_glob='{fields_glob}'")

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
            print(f"[build_field_mukey_map][WARN] fields_path not found, falling back to fields_glob: {p}")

    if str(fields_glob or "").strip():
        matches = sorted(glob.glob(str(fields_glob), recursive=True))
        if not matches:
            matches = sorted(glob.glob(str((_resolve(".") / str(fields_glob)).as_posix()), recursive=True))
        _vlog(verbose, f"fields_glob matched files={len(matches)}")
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

    fields = fields[fields.geometry.notna() & ~fields.geometry.is_empty].copy()
    _vlog(verbose, f"loaded field polygons rows={len(fields)} files={len(input_files)}")
    return fields, input_files


def _list_vector_layers(path: Path) -> list[str]:
    # Best-effort layer discovery for container datasets (for example .gdb, .gpkg).
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
        # pyogrio may return a 2D ndarray/list where row[0] is layer name.
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


def build_field_mukey_map(
    *,
    fields_path: str,
    fields_glob: str,
    ssurgo_path: str,
    ssurgo_glob: str,
    ssurgo_layer: str,
    field_id_field: str,
    mukey_field: str,
    output_csv: str,
    output_long_csv: str,
    summary_json: str,
    target_crs: str,
    verbose: bool,
) -> dict[str, Any]:
    try:
        import geopandas as gpd
        import pandas as pd
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("build_field_mukey_map requires geopandas and pandas") from exc

    fields, input_files = _load_fields(fields_path, fields_glob, field_id_field, verbose)
    _vlog(verbose, "resolving SSURGO inputs")
    ssurgo_files: list[Path] = []
    ssurgo_path_resolved = _resolve(ssurgo_path) if str(ssurgo_path or "").strip() else None
    if ssurgo_path_resolved is not None:
        if ssurgo_path_resolved.exists():
            ssurgo_files.append(ssurgo_path_resolved)
        elif not str(ssurgo_glob or "").strip():
            raise FileNotFoundError(f"ssurgo_path not found: {ssurgo_path_resolved}")
        elif verbose:
            print(f"[build_field_mukey_map][WARN] ssurgo_path not found, falling back to ssurgo_glob: {ssurgo_path_resolved}")

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
            # Accept regular vector files and directory vector datasets (e.g., .gdb).
            if p.is_file() or (p.is_dir() and p.suffix.lower() in {".gdb"}):
                if p not in ssurgo_files:
                    ssurgo_files.append(p)
    if not ssurgo_files:
        raise FileNotFoundError("no SSURGO files found via ssurgo_path/ssurgo_glob")
    _vlog(verbose, f"SSURGO input candidates files={len(ssurgo_files)}")

    ssurgo_frames: list[Any] = []
    ssurgo_loaded_files: list[str] = []
    for p in ssurgo_files:
        # Per-file layer candidates:
        # 1) explicit user-provided layer
        # 2) common gSSURGO polygon names
        # 3) discovered layers from the container
        # 4) default layer fallback
        layer_candidates: list[str] = []
        if str(ssurgo_layer or "").strip():
            layer_candidates.append(str(ssurgo_layer or "").strip())
        layer_candidates.extend(["MUPOLYGON", "mupolygon", "MapunitPoly", "mapunit", "ssurgo_mapunit"])
        if p.suffix.lower() in {".gdb", ".gpkg"}:
            discovered = _list_vector_layers(p)
            layer_candidates.extend(discovered)
            _vlog(
                verbose,
                f"discovered container layers file={p.as_posix()} count={len(discovered)}",
            )
        layer_candidates.append("")  # final fallback: default layer for file
        deduped_candidates: list[str] = []
        seen_layer_keys: set[str] = set()
        for item in layer_candidates:
            key = str(item).strip().lower()
            if key in seen_layer_keys:
                continue
            seen_layer_keys.add(key)
            deduped_candidates.append(str(item))

        loaded = False
        last_err = ""
        for layer_name in deduped_candidates:
            _vlog(verbose, f"trying SSURGO file={p.as_posix()} layer={layer_name or '<default>'}")
            try:
                if layer_name:
                    cand = gpd.read_file(p, layer=layer_name)
                else:
                    cand = gpd.read_file(p)
            except Exception as exc:  # noqa: BLE001
                last_err = str(exc)
                continue
            if cand is None or cand.empty:
                continue
            actual_mukey_col = None
            by_lower = {str(c).lower(): c for c in cand.columns}
            if mukey_field in cand.columns:
                actual_mukey_col = mukey_field
            else:
                actual_mukey_col = by_lower.get(str(mukey_field).lower())
            if actual_mukey_col is None:
                continue
            if cand.crs is None:
                continue
            cand = cand[[actual_mukey_col, "geometry"]].copy()
            if actual_mukey_col != mukey_field:
                cand = cand.rename(columns={actual_mukey_col: mukey_field})
            cand[mukey_field] = cand[mukey_field].map(_to_text)
            cand = cand[cand.geometry.notna() & ~cand.geometry.is_empty].copy()
            if cand.empty:
                continue
            ssurgo_frames.append(cand)
            ssurgo_loaded_files.append(p.as_posix())
            loaded = True
            lname = layer_name or "<default>"
            _vlog(
                verbose,
                f"loaded ssurgo file={p.as_posix()} layer={lname} mukey_col={actual_mukey_col} rows={len(cand)}",
            )
            break
        if not loaded and verbose:
            print(f"[build_field_mukey_map][WARN] no usable mukey polygon layer in {p.as_posix()} err={last_err[:200]}")

    if not ssurgo_frames:
        raise RuntimeError("no usable SSURGO polygon layers loaded; check ssurgo_glob/ssurgo_layer and mukey_field")

    ssurgo = gpd.GeoDataFrame(pd.concat(ssurgo_frames, ignore_index=True), geometry="geometry")
    if ssurgo.crs is None:
        # Use first frame CRS if concat lost metadata.
        ssurgo = gpd.GeoDataFrame(ssurgo, geometry="geometry", crs=ssurgo_frames[0].crs)

    if target_crs.strip():
        _vlog(verbose, f"projecting inputs to target_crs={target_crs}")
        fields = fields.to_crs(target_crs)
        ssurgo = ssurgo.to_crs(target_crs)
    else:
        _vlog(verbose, f"projecting fields to SSURGO CRS={ssurgo.crs}")
        fields = fields.to_crs(ssurgo.crs)

    ssurgo = ssurgo[[mukey_field, "geometry"]].copy()
    ssurgo[mukey_field] = ssurgo[mukey_field].map(_to_text)
    ssurgo = ssurgo[ssurgo.geometry.notna() & ~ssurgo.geometry.is_empty].copy()

    _vlog(verbose, f"prepared SSURGO polygons rows={len(ssurgo)}")

    _vlog(verbose, "running spatial join (intersects)")
    joined = gpd.sjoin(
        fields[[field_id_field, "source_name", "geometry"]],
        ssurgo,
        how="inner",
        predicate="intersects",
    )
    if joined.empty:
        raise RuntimeError("no field polygons intersect SSURGO polygons")
    _vlog(verbose, f"spatial join rows={len(joined)}")
    # Compute overlap area and percent overlap of each field polygon by mukey.
    field_src = fields[[field_id_field, "source_name", "geometry"]].copy()
    field_src[field_id_field] = field_src[field_id_field].map(_to_text)
    field_src = field_src[field_src[field_id_field].astype(str).str.len() > 0].copy()
    field_src["field_area"] = field_src.geometry.area

    ssurgo_src = ssurgo[[mukey_field, "geometry"]].copy()
    ssurgo_src[mukey_field] = ssurgo_src[mukey_field].map(_to_text)
    ssurgo_src = ssurgo_src[ssurgo_src[mukey_field].astype(str).str.len() > 0].copy()

    _vlog(verbose, "running polygon overlay (intersection)")
    intersections = gpd.overlay(field_src, ssurgo_src, how="intersection")
    if intersections.empty:
        raise RuntimeError("field and SSURGO geometries intersect by bbox but produced no polygon intersections")
    _vlog(verbose, f"overlay rows={len(intersections)}")
    intersections["overlap_area"] = intersections.geometry.area

    grouped_pairs = (
        intersections.groupby([field_id_field, mukey_field], as_index=False)["overlap_area"]
        .sum()
    )
    field_area_map = (
        field_src.groupby(field_id_field, as_index=False)["field_area"]
        .sum()
        .set_index(field_id_field)["field_area"]
        .to_dict()
    )
    source_name_map = (
        field_src.groupby(field_id_field, as_index=False)["source_name"]
        .first()
        .set_index(field_id_field)["source_name"]
        .to_dict()
    )

    pair_rows: list[dict[str, Any]] = []
    for _, row in grouped_pairs.iterrows():
        field_id = _to_text(row.get(field_id_field))
        mukey = _to_text(row.get(mukey_field))
        overlap_area = float(row.get("overlap_area") or 0.0)
        field_area = float(field_area_map.get(field_id, 0.0) or 0.0)
        pct_field_overlap = (overlap_area / field_area * 100.0) if field_area > 0 else 0.0
        pair_rows.append(
            {
                field_id_field: field_id,
                mukey_field: mukey,
                "source_name": _to_text(source_name_map.get(field_id, "")),
                "overlap_area": overlap_area,
                "field_area": field_area,
                "pct_field_overlap": pct_field_overlap,
            }
        )

    pair_rows.sort(key=lambda r: (str(r.get(field_id_field) or ""), str(r.get(mukey_field) or "")))

    grouped: dict[str, dict[str, Any]] = {}
    for row in pair_rows:
        field_id = _to_text(row.get(field_id_field))
        mukey = _to_text(row.get(mukey_field))
        source_name = _to_text(row.get("source_name"))
        overlap_area = float(row.get("overlap_area") or 0.0)
        pct_field_overlap = float(row.get("pct_field_overlap") or 0.0)
        item = grouped.setdefault(
            field_id,
            {
                field_id_field: field_id,
                "source_name": source_name,
                "field_area": float(row.get("field_area") or 0.0),
                "mukeys": [],
                "mukey_pct": {},
                "mukey_overlap_area": {},
            },
        )
        if mukey not in item["mukeys"]:
            item["mukeys"].append(mukey)
        item["mukey_pct"][mukey] = pct_field_overlap
        item["mukey_overlap_area"][mukey] = overlap_area

    output_csv_path = _resolve(output_csv)
    output_long_path = _resolve(output_long_csv)
    summary_path = _resolve(summary_json)
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    output_long_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    _vlog(verbose, f"writing outputs csv={output_csv_path.as_posix()} long_csv={output_long_path.as_posix()}")

    with output_long_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[field_id_field, mukey_field, "source_name", "overlap_area", "field_area", "pct_field_overlap"],
        )
        w.writeheader()
        for row in pair_rows:
            w.writerow(row)

    with output_csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
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
        for field_id in sorted(grouped.keys()):
            item = grouped[field_id]
            mukeys = sorted(item["mukeys"])
            pct_map = {mk: item["mukey_pct"].get(mk, 0.0) for mk in mukeys}
            overlap_map = {mk: item["mukey_overlap_area"].get(mk, 0.0) for mk in mukeys}
            w.writerow(
                {
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
            "fields_path": str(fields_path or ""),
            "fields_glob": str(fields_glob or ""),
            "ssurgo_path": ssurgo_path_resolved.as_posix() if ssurgo_path_resolved is not None else "",
            "ssurgo_glob": str(ssurgo_glob or ""),
            "ssurgo_layer": str(ssurgo_layer or ""),
            "ssurgo_loaded_files": ssurgo_loaded_files,
            "input_files": input_files,
        },
        "counts": {
            "field_rows": int(len(fields)),
            "ssurgo_rows": int(len(ssurgo)),
            "join_rows": int(len(joined)),
            "intersection_rows": int(len(intersections)),
            "unique_field_mukey_pairs": int(len(pair_rows)),
            "unique_fields": int(len(grouped)),
        },
        "outputs": {
            "output_csv": output_csv_path.as_posix(),
            "output_long_csv": output_long_path.as_posix(),
            "summary_json": summary_path.as_posix(),
        },
        "field_id_field": field_id_field,
        "mukey_field": mukey_field,
        "target_crs": target_crs or str(ssurgo.crs),
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    _vlog(
        verbose,
        f"complete unique_fields={summary['counts']['unique_fields']} "
        f"unique_pairs={summary['counts']['unique_field_mukey_pairs']}",
    )
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Find all SSURGO mukeys intersecting each field polygon (supports batch by field polygon files)."
    )
    ap.add_argument("--fields-path", default="", help="Single field polygons vector path")
    ap.add_argument("--fields-glob", default="", help="Glob for batch field polygon vectors")
    ap.add_argument("--ssurgo-path", required=True, help="SSURGO polygon layer path with mukey column")
    ap.add_argument("--ssurgo-glob", default="", help="Glob for SSURGO polygon files when ssurgo-path is missing or split by state")
    ap.add_argument("--ssurgo-layer", default="", help="Optional layer name when --ssurgo-path is a container (e.g. .gdb/.gpkg)")
    ap.add_argument("--field-id-field", default="tile_field_id")
    ap.add_argument("--mukey-field", default="mukey")
    ap.add_argument("--output-csv", required=True, help="Field-level mapping CSV with mukeys_json")
    ap.add_argument("--output-long-csv", required=True, help="Long mapping CSV: one row per field_id,mukey")
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--target-crs", default="EPSG:5070")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    build_field_mukey_map(
        fields_path=str(args.fields_path or ""),
        fields_glob=str(args.fields_glob or ""),
        ssurgo_path=str(args.ssurgo_path),
        ssurgo_glob=str(args.ssurgo_glob or ""),
        ssurgo_layer=str(args.ssurgo_layer or ""),
        field_id_field=str(args.field_id_field),
        mukey_field=str(args.mukey_field),
        output_csv=str(args.output_csv),
        output_long_csv=str(args.output_long_csv),
        summary_json=str(args.summary_json),
        target_crs=str(args.target_crs or ""),
        verbose=bool(args.verbose),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
