#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Iterable

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.mask import mask


TILE_RE = re.compile(r"(?i)^h\d{2}v\d{2}$")


def _resolve(path_text: str) -> Path:
    return Path(path_text).expanduser().resolve()


def _iter_rasters(prism_dir: Path) -> Iterable[Path]:
    for path in sorted(prism_dir.rglob("*")):
        if path.is_file() and path.suffix.lower() in {".tif", ".tiff"}:
            yield path


def _aggregate_polygon(ds: rasterio.io.DatasetReader, geom) -> tuple[int, dict[str, float]]:
    try:
        data, _ = mask(ds, [geom], crop=True, filled=False)
    except ValueError:
        return 0, {}
    band = np.ma.array(data[0], copy=False)
    if band.count() == 0:
        return 0, {}
    values = band.compressed().astype(float)
    return int(values.size), {"mean": float(np.mean(values)), "max": float(np.max(values))}


def main() -> int:
    ap = argparse.ArgumentParser(description="Aggregate PRISM vpdmax rasters for one YanRoy tile.")
    ap.add_argument("--tile", required=True)
    ap.add_argument("--prism-dir", required=True)
    ap.add_argument("--polygon-path", required=True)
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--polygon-id-field", default="field_id")
    ap.add_argument("--day-from-filename-regex", default=r"(20\d{2}(0[1-9]|1[0-2]))")
    ap.add_argument("--day-from-filename-group", type=int, default=1)
    ap.add_argument("--value-prefix", default="vpdmax")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    tile = str(args.tile or "").strip().lower()
    if not TILE_RE.match(tile):
        raise ValueError(f"invalid tile: {tile}")

    prism_dir = _resolve(args.prism_dir)
    polygon_path = _resolve(args.polygon_path)
    output_csv = _resolve(args.output_csv)
    summary_json = _resolve(args.summary_json)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    if not prism_dir.exists():
        raise FileNotFoundError(f"prism dir not found: {prism_dir}")
    if not polygon_path.exists():
        raise FileNotFoundError(f"polygon path not found: {polygon_path}")

    polygons = gpd.read_file(polygon_path)
    if polygons.empty:
        raise RuntimeError(f"polygon path contains no features: {polygon_path}")
    if args.polygon_id_field not in polygons.columns:
        raise RuntimeError(f"polygon_id_field not found: {args.polygon_id_field}")

    day_pattern = re.compile(str(args.day_from_filename_regex))
    rows: list[dict[str, object]] = []
    raster_count = 0
    skipped_rasters = 0

    for raster_path in _iter_rasters(prism_dir):
        match = day_pattern.search(raster_path.name)
        if not match:
            skipped_rasters += 1
            continue
        day_value = str(match.group(args.day_from_filename_group) or "").strip()
        if not day_value:
            skipped_rasters += 1
            continue

        with rasterio.open(raster_path) as ds:
            if ds.crs is None:
                raise RuntimeError(f"raster missing CRS: {raster_path}")
            tile_polygons = polygons.to_crs(ds.crs) if polygons.crs and polygons.crs != ds.crs else polygons
            for _, polygon_row in tile_polygons.iterrows():
                pixel_count, stats = _aggregate_polygon(ds, polygon_row.geometry)
                if pixel_count == 0:
                    continue
                out_row = {
                    "field_id": str(polygon_row[args.polygon_id_field]),
                    "day": day_value,
                    "tile_coord": tile,
                    f"{args.value_prefix}_mean": f"{stats['mean']:.6f}",
                    f"{args.value_prefix}_max": f"{stats['max']:.6f}",
                    "pixel_count": pixel_count,
                }
                rows.append(out_row)
        raster_count += 1
        if args.verbose:
            print(f"[aggregate_vpdmax_by_tile] tile={tile} raster={raster_path.name} rows={len(rows)}")

    rows.sort(key=lambda row: (str(row["day"]), str(row["field_id"])))
    fieldnames = [
        "field_id",
        "day",
        "tile_coord",
        f"{args.value_prefix}_mean",
        f"{args.value_prefix}_max",
        "pixel_count",
    ]
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    summary = {
        "tile": tile,
        "prism_dir": prism_dir.as_posix(),
        "polygon_path": polygon_path.as_posix(),
        "output_csv": output_csv.as_posix(),
        "row_count": len(rows),
        "raster_count": raster_count,
        "skipped_rasters": skipped_rasters,
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if args.verbose:
        print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
