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
    if verbose:
        print(f"[build_field_mukey_map] loaded fields rows={len(fields)} files={len(input_files)}")
    return fields, input_files


def build_field_mukey_map(
    *,
    fields_path: str,
    fields_glob: str,
    ssurgo_path: str,
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
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("build_field_mukey_map requires geopandas") from exc

    fields, input_files = _load_fields(fields_path, fields_glob, field_id_field, verbose)
    ssurgo_path_resolved = _resolve(ssurgo_path)
    if ssurgo_layer.strip():
        ssurgo = gpd.read_file(ssurgo_path_resolved, layer=ssurgo_layer.strip())
    else:
        ssurgo = gpd.read_file(ssurgo_path_resolved)
    if ssurgo.empty:
        raise RuntimeError(f"ssurgo input is empty: {ssurgo_path}")
    if ssurgo.crs is None:
        raise RuntimeError("ssurgo input missing CRS")
    if mukey_field not in ssurgo.columns:
        raise ValueError(f"mukey_field not found in ssurgo input: {mukey_field}")

    if target_crs.strip():
        fields = fields.to_crs(target_crs)
        ssurgo = ssurgo.to_crs(target_crs)
    else:
        fields = fields.to_crs(ssurgo.crs)

    ssurgo = ssurgo[[mukey_field, "geometry"]].copy()
    ssurgo[mukey_field] = ssurgo[mukey_field].map(_to_text)
    ssurgo = ssurgo[ssurgo.geometry.notna() & ~ssurgo.geometry.is_empty].copy()

    joined = gpd.sjoin(
        fields[[field_id_field, "source_name", "geometry"]],
        ssurgo,
        how="inner",
        predicate="intersects",
    )
    if joined.empty:
        raise RuntimeError("no field polygons intersect SSURGO polygons")
    # Compute overlap area and percent overlap of each field polygon by mukey.
    field_src = fields[[field_id_field, "source_name", "geometry"]].copy()
    field_src[field_id_field] = field_src[field_id_field].map(_to_text)
    field_src = field_src[field_src[field_id_field].astype(str).str.len() > 0].copy()
    field_src["field_area"] = field_src.geometry.area

    ssurgo_src = ssurgo[[mukey_field, "geometry"]].copy()
    ssurgo_src[mukey_field] = ssurgo_src[mukey_field].map(_to_text)
    ssurgo_src = ssurgo_src[ssurgo_src[mukey_field].astype(str).str.len() > 0].copy()

    intersections = gpd.overlay(field_src, ssurgo_src, how="intersection")
    if intersections.empty:
        raise RuntimeError("field and SSURGO geometries intersect by bbox but produced no polygon intersections")
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
            "ssurgo_path": ssurgo_path_resolved.as_posix(),
            "ssurgo_layer": str(ssurgo_layer or ""),
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
    if verbose:
        print(
            f"[build_field_mukey_map] fields={summary['counts']['unique_fields']} "
            f"pairs={summary['counts']['unique_field_mukey_pairs']}"
        )
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Find all SSURGO mukeys intersecting each field polygon (supports batch by field polygon files)."
    )
    ap.add_argument("--fields-path", default="", help="Single field polygons vector path")
    ap.add_argument("--fields-glob", default="", help="Glob for batch field polygon vectors")
    ap.add_argument("--ssurgo-path", required=True, help="SSURGO polygon layer path with mukey column")
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
