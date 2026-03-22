#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any


_TILE_RE = re.compile(r"(?i)\b(h\d{2}v\d{2})\b")


def _to_scalar(value: Any):
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:  # noqa: BLE001
            return value
    return value


def _parse_years(raw: str) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for token in str(raw or "").replace(";", ",").split(","):
        text = token.strip()
        if not text:
            continue
        year = int(text)
        if year in seen:
            continue
        seen.add(year)
        out.append(year)
    return out


def _parse_year_from_name(path: Path, pattern: re.Pattern[str]) -> int | None:
    match = pattern.search(path.name)
    if not match:
        return None
    try:
        return int(match.group("year"))
    except Exception:  # noqa: BLE001
        return None


def _derive_tile_coord(row: dict[str, Any], tile_field: str, source_name_field: str) -> str:
    tile_value = _to_scalar(row.get(tile_field)) if tile_field in row else None
    if tile_value not in (None, ""):
        return str(tile_value).strip().lower()
    source_value = _to_scalar(row.get(source_name_field)) if source_name_field in row else None
    if source_value not in (None, ""):
        match = _TILE_RE.search(str(source_value))
        if match:
            return match.group(1).lower()
    return ""


def _derive_field_id(row: dict[str, Any], field_field: str) -> str:
    value = _to_scalar(row.get(field_field)) if field_field in row else None
    if value in (None, ""):
        return ""
    return str(value).strip()


def _derive_tile_field_id(
    row: dict[str, Any],
    *,
    id_field: str,
    tile_field: str,
    field_field: str,
    source_name_field: str,
) -> str:
    explicit = _to_scalar(row.get(id_field)) if id_field in row else None
    if explicit not in (None, ""):
        return str(explicit).strip()
    tile_coord = _derive_tile_coord(row, tile_field, source_name_field)
    field_id = _derive_field_id(row, field_field)
    if tile_coord and field_id:
        return f"{tile_coord}_{field_id}"
    return ""


def _iter_rasters(input_dir: Path, suffixes: set[str]) -> list[Path]:
    return sorted(
        p.resolve()
        for p in input_dir.rglob("*")
        if p.is_file() and p.suffix.lower() in suffixes
    )


def aggregate_lobell_corn_to_fields(
    *,
    input_dir: Path,
    polygon_path: Path,
    output_csv: Path,
    summary_json: Path,
    years: list[int],
    filename_regex: str,
    id_field: str,
    tile_field: str,
    field_field: str,
    source_name_field: str,
    verbose: bool,
) -> dict[str, Any]:
    try:
        import geopandas as gpd
        import numpy as np
        import rasterio
        from rasterio.io import MemoryFile
        from rasterio.mask import mask
        from rasterio.merge import merge
        from shapely.geometry import box
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "aggregate_lobell_corn_to_fields requires geopandas, numpy, rasterio, and shapely"
        ) from exc

    if not input_dir.exists():
        raise FileNotFoundError(f"input dir not found: {input_dir}")
    if not polygon_path.exists():
        raise FileNotFoundError(f"polygon path not found: {polygon_path}")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    pattern = re.compile(str(filename_regex))
    all_rasters = _iter_rasters(input_dir, suffixes={".tif", ".tiff"})
    rasters_by_year: dict[int, list[Path]] = {year: [] for year in years}
    for path in all_rasters:
        year = _parse_year_from_name(path, pattern)
        if year is None or year not in rasters_by_year:
            continue
        rasters_by_year[year].append(path)

    gdf = gpd.read_file(polygon_path)
    if gdf.empty:
        raise RuntimeError(f"polygon file has no features: {polygon_path}")
    if gdf.crs is None:
        raise RuntimeError("polygon file missing CRS")

    rows: list[dict[str, Any]] = []
    year_stats: list[dict[str, Any]] = []

    for year in years:
        raster_paths = rasters_by_year.get(year, [])
        if not raster_paths:
            year_stats.append({"year": year, "raster_count": 0, "row_count": 0, "status": "missing"})
            continue

        srcs = [rasterio.open(path) for path in raster_paths]
        try:
            mosaic, transform = merge(srcs)
            template = srcs[0]
            meta = template.meta.copy()
            meta.update(
                {
                    "driver": "GTiff",
                    "height": int(mosaic.shape[1]),
                    "width": int(mosaic.shape[2]),
                    "transform": transform,
                    "count": int(mosaic.shape[0]),
                }
            )

            with MemoryFile() as memfile:
                with memfile.open(**meta) as ds:
                    ds.write(mosaic)
                    polygons = gdf.to_crs(ds.crs)
                    raster_bounds = box(float(ds.bounds.left), float(ds.bounds.bottom), float(ds.bounds.right), float(ds.bounds.top))
                    polygons = polygons[polygons.geometry.notna() & ~polygons.geometry.is_empty].copy()
                    polygons = polygons[polygons.geometry.intersects(raster_bounds)].copy()

                    year_row_count = 0
                    for _, feature in polygons.iterrows():
                        tile_coord = _derive_tile_coord(feature, tile_field, source_name_field)
                        field_id = _derive_field_id(feature, field_field)
                        tile_field_id = _derive_tile_field_id(
                            feature,
                            id_field=id_field,
                            tile_field=tile_field,
                            field_field=field_field,
                            source_name_field=source_name_field,
                        )
                        if not tile_field_id:
                            continue
                        try:
                            masked, _ = mask(ds, [feature.geometry], crop=True, indexes=1, filled=False, all_touched=False)
                            values = np.ma.asarray(masked).compressed()
                        except ValueError:
                            values = np.array([])
                        if values.size == 0:
                            continue
                        mean_value = float(np.mean(values))
                        rows.append(
                            {
                                "tile_coord": tile_coord,
                                "field_ID": field_id,
                                "tile_field_ID": tile_field_id,
                                "year": year,
                                "unscaled_yield": mean_value,
                                "source_raster_count": len(raster_paths),
                                "pixel_count": int(values.size),
                            }
                        )
                        year_row_count += 1

                    year_stats.append(
                        {
                            "year": year,
                            "raster_count": len(raster_paths),
                            "row_count": year_row_count,
                            "status": "ok",
                        }
                    )
                    if verbose:
                        print(
                            f"[aggregate_lobell_corn_to_fields] year={year} rasters={len(raster_paths)} rows={year_row_count}"
                        )
        finally:
            for src in srcs:
                src.close()

    rows.sort(key=lambda r: (str(r["tile_field_ID"]), int(r["year"])))
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "tile_coord",
                "field_ID",
                "tile_field_ID",
                "year",
                "unscaled_yield",
                "source_raster_count",
                "pixel_count",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    duplicates = len(rows) - len({(str(r["tile_field_ID"]), int(r["year"])) for r in rows})
    summary = {
        "input_dir": input_dir.as_posix(),
        "polygon_path": polygon_path.as_posix(),
        "output_csv": output_csv.as_posix(),
        "requested_years": years,
        "filename_regex": filename_regex,
        "total_raster_count": len(all_rasters),
        "matched_raster_count": sum(len(v) for v in rasters_by_year.values()),
        "row_count": len(rows),
        "duplicate_tile_field_year_rows": duplicates,
        "year_stats": year_stats,
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Aggregate raw Lobell corn rasters to YanRoy field-year rows."
    )
    ap.add_argument("--input-dir", required=True)
    ap.add_argument("--polygon-path", required=True)
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--years", required=True, help="Comma-separated years to process.")
    ap.add_argument("--filename-regex", default=r"(?P<year>20\d{2})")
    ap.add_argument("--id-field", default="tile_field_id")
    ap.add_argument("--tile-field", default="tile_id")
    ap.add_argument("--field-field", default="field_id")
    ap.add_argument("--source-name-field", default="source_name")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    summary = aggregate_lobell_corn_to_fields(
        input_dir=Path(args.input_dir).expanduser().resolve(),
        polygon_path=Path(args.polygon_path).expanduser().resolve(),
        output_csv=Path(args.output_csv).expanduser().resolve(),
        summary_json=Path(args.summary_json).expanduser().resolve(),
        years=_parse_years(args.years),
        filename_regex=str(args.filename_regex),
        id_field=str(args.id_field),
        tile_field=str(args.tile_field),
        field_field=str(args.field_field),
        source_name_field=str(args.source_name_field),
        verbose=bool(args.verbose),
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
