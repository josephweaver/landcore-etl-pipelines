#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import uuid
from pathlib import Path
from typing import Any

FIELD_NAMESPACE = uuid.UUID("9e6c1b5a-84d9-49c6-89df-cf8f1ca0af31")
BOUNDARY_NAMESPACE = uuid.UUID("8a0f82f6-e540-4fc8-8f46-b6ce1c0df723")
TILE_RE = re.compile(r"(?i)(h\d{2}v\d{2})")


def _to_scalar(value: Any):
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:  # noqa: BLE001
            return value
    return value


def _normalize_text(value: Any) -> str:
    return str(_to_scalar(value) or "").strip()


def _normalize_date(value: str) -> str | None:
    text = _normalize_text(value)
    return text or None


def _extract_tile_coord(*, row: Any, tile_field: str, source_name_field: str) -> str:
    direct = _normalize_text(row.get(tile_field)) if tile_field in row else ""
    if direct:
        return direct.lower()
    source_name = _normalize_text(row.get(source_name_field)) if source_name_field in row else ""
    if source_name:
        match = TILE_RE.search(source_name)
        if match:
            return match.group(1).lower()
    return ""


def _geometry_hash(geom: Any) -> str:
    payload = getattr(geom, "wkb", b"")
    return hashlib.sha256(payload).hexdigest()


def _field_id(source: str, tile_coord: str, yanroy_field_id: str) -> str:
    return str(uuid.uuid5(FIELD_NAMESPACE, f"{source}|{tile_coord}|{yanroy_field_id}"))


def _field_boundary_id(
    source: str,
    resolution: str,
    valid_start: str | None,
    valid_end: str | None,
    tile_coord: str,
    yanroy_field_id: str,
    geometry_hash: str,
) -> str:
    return str(
        uuid.uuid5(
            BOUNDARY_NAMESPACE,
            "|".join(
                [
                    source,
                    resolution,
                    valid_start or "",
                    valid_end or "",
                    tile_coord,
                    yanroy_field_id,
                    geometry_hash,
                ]
            ),
        )
    )


def build_db_fields(
    *,
    input_vector: Path,
    field_csv: Path,
    field_parquet: Path,
    field_boundary_csv: Path,
    field_boundary_parquet: Path,
    field_boundary_gpkg: Path,
    summary_json: Path,
    source: str,
    resolution: str,
    valid_start: str | None,
    valid_end: str | None,
    field_id_field: str,
    tile_field: str,
    source_name_field: str,
    verbose: bool = False,
) -> dict[str, Any]:
    try:
        import geopandas as gpd
        import pandas as pd
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("build_db_fields requires geopandas and pandas") from exc

    if not input_vector.exists():
        raise FileNotFoundError(f"input vector not found: {input_vector}")

    gdf = gpd.read_file(input_vector)
    if gdf.empty:
        raise RuntimeError(f"input vector has no features: {input_vector}")
    if gdf.crs is None:
        raise RuntimeError("input vector missing CRS")
    if field_id_field not in gdf.columns:
        raise RuntimeError(f"input vector missing field id column: {field_id_field}")

    boundary_rows: list[dict[str, Any]] = []
    fields_index: dict[str, dict[str, Any]] = {}
    skipped = 0

    for _, row in gdf.iterrows():
        geom = row.get("geometry")
        if geom is None or getattr(geom, "is_empty", True):
            skipped += 1
            continue
        yanroy_field_id = _normalize_text(row.get(field_id_field))
        tile_coord = _extract_tile_coord(row=row, tile_field=tile_field, source_name_field=source_name_field)
        source_name = _normalize_text(row.get(source_name_field)) if source_name_field in row else ""
        if not yanroy_field_id or not tile_coord:
            skipped += 1
            continue

        field_id = _field_id(source, tile_coord, yanroy_field_id)
        geom_hash = _geometry_hash(geom)
        field_boundary_id = _field_boundary_id(
            source=source,
            resolution=resolution,
            valid_start=valid_start,
            valid_end=valid_end,
            tile_coord=tile_coord,
            yanroy_field_id=yanroy_field_id,
            geometry_hash=geom_hash,
        )
        legacy_tile_field_id = f"{tile_coord}_{yanroy_field_id}"

        boundary_rows.append(
            {
                "field_boundary_id": field_boundary_id,
                "field_id": field_id,
                "source": source,
                "resolution": resolution,
                "valid_start": valid_start or "",
                "valid_end": valid_end or "",
                "tile_coord": tile_coord,
                "yanroy_field_id": yanroy_field_id,
                "source_name": source_name,
                "legacy_tile_field_id": legacy_tile_field_id,
                "geometry_hash": geom_hash,
                "geometry": geom,
            }
        )

        current = fields_index.get(field_id)
        if current is None:
            fields_index[field_id] = {
                "field_id": field_id,
                "source": source,
                "tile_coord": tile_coord,
                "yanroy_field_id": yanroy_field_id,
                "source_names": [source_name] if source_name else [],
                "boundary_count": 1,
            }
        else:
            current["boundary_count"] = int(current.get("boundary_count") or 0) + 1
            if source_name and source_name not in current["source_names"]:
                current["source_names"].append(source_name)

    field_rows: list[dict[str, Any]] = []
    for row in sorted(fields_index.values(), key=lambda x: (str(x["tile_coord"]), str(x["yanroy_field_id"]))):
        field_rows.append(
            {
                "field_id": row["field_id"],
                "source": row["source"],
                "tile_coord": row["tile_coord"],
                "yanroy_field_id": row["yanroy_field_id"],
                "source_names": ";".join(sorted(row["source_names"])),
                "boundary_count": int(row["boundary_count"]),
            }
        )

    if not boundary_rows:
        raise RuntimeError(
            "no boundary rows were produced; verify input field_id/source_name columns and tile parsing"
        )

    field_df = pd.DataFrame(field_rows)
    boundary_df = pd.DataFrame(boundary_rows)
    boundary_gdf = gpd.GeoDataFrame(boundary_df, geometry="geometry", crs=gdf.crs)

    field_csv.parent.mkdir(parents=True, exist_ok=True)
    field_parquet.parent.mkdir(parents=True, exist_ok=True)
    field_boundary_csv.parent.mkdir(parents=True, exist_ok=True)
    field_boundary_parquet.parent.mkdir(parents=True, exist_ok=True)
    field_boundary_gpkg.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    field_df.to_csv(field_csv, index=False, encoding="utf-8")
    field_df.to_parquet(field_parquet, index=False)

    boundary_gdf.drop(columns=["geometry"]).to_csv(field_boundary_csv, index=False, encoding="utf-8")
    boundary_gdf.to_parquet(field_boundary_parquet, index=False)
    boundary_gdf.to_file(field_boundary_gpkg, driver="GPKG")

    duplicate_field_ids = len(field_rows) - len({r["field_id"] for r in field_rows})
    duplicate_boundary_ids = len(boundary_rows) - len({r["field_boundary_id"] for r in boundary_rows})
    summary = {
        "input_vector": input_vector.resolve().as_posix(),
        "field_csv": field_csv.resolve().as_posix(),
        "field_parquet": field_parquet.resolve().as_posix(),
        "field_boundary_csv": field_boundary_csv.resolve().as_posix(),
        "field_boundary_parquet": field_boundary_parquet.resolve().as_posix(),
        "field_boundary_gpkg": field_boundary_gpkg.resolve().as_posix(),
        "field_count": len(field_rows),
        "field_boundary_count": len(boundary_rows),
        "duplicate_field_ids": duplicate_field_ids,
        "duplicate_field_boundary_ids": duplicate_boundary_ids,
        "skipped_features": skipped,
        "source": source,
        "resolution": resolution,
        "valid_start": valid_start,
        "valid_end": valid_end,
        "crs": str(gdf.crs),
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if verbose:
        print(
            f"[build_db_fields] fields={len(field_rows)} "
            f"boundaries={len(boundary_rows)} skipped={skipped} "
            f"output={field_boundary_parquet.as_posix()}"
        )
    return summary


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build canonical field and field_boundary tables from YanRoy polygons.")
    ap.add_argument("--input-vector", required=True)
    ap.add_argument("--field-csv", required=True)
    ap.add_argument("--field-parquet", required=True)
    ap.add_argument("--field-boundary-csv", required=True)
    ap.add_argument("--field-boundary-parquet", required=True)
    ap.add_argument("--field-boundary-gpkg", required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--source", default="yanroy")
    ap.add_argument("--resolution", default="30m")
    ap.add_argument("--valid-start", default="")
    ap.add_argument("--valid-end", default="")
    ap.add_argument("--field-id-field", default="field_id")
    ap.add_argument("--tile-field", default="tile_id")
    ap.add_argument("--source-name-field", default="source_name")
    ap.add_argument("--verbose", action="store_true")
    args, unknown = ap.parse_known_args(argv)
    if unknown:
        print(f"[build_db_fields][WARN] ignoring unknown args: {' '.join(unknown)}")

    build_db_fields(
        input_vector=Path(args.input_vector).expanduser().resolve(),
        field_csv=Path(args.field_csv).expanduser().resolve(),
        field_parquet=Path(args.field_parquet).expanduser().resolve(),
        field_boundary_csv=Path(args.field_boundary_csv).expanduser().resolve(),
        field_boundary_parquet=Path(args.field_boundary_parquet).expanduser().resolve(),
        field_boundary_gpkg=Path(args.field_boundary_gpkg).expanduser().resolve(),
        summary_json=Path(args.summary_json).expanduser().resolve(),
        source=str(args.source).strip() or "yanroy",
        resolution=str(args.resolution).strip() or "30m",
        valid_start=_normalize_date(str(args.valid_start)),
        valid_end=_normalize_date(str(args.valid_end)),
        field_id_field=str(args.field_id_field).strip() or "field_id",
        tile_field=str(args.tile_field).strip() or "tile_id",
        source_name_field=str(args.source_name_field).strip() or "source_name",
        verbose=bool(args.verbose),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
