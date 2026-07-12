#!/usr/bin/env python3
"""Count aligned field/crop raster value pairs with GDAL block reads and Numpy."""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import time
from pathlib import Path
from typing import Any

import numpy as np
from osgeo import gdal

from field_crop_common import ensure_parent_dir


gdal.UseExceptions()

COUNTS_ARTIFACT = "field_crop_year_counts.csv"
COUNTS_METADATA_ARTIFACT = "field_crop_year_counts.metadata.json"
COUNTS_REQUEST_ARTIFACT = "field_crop_year_counts.request.json"
UINT32_MAX = (1 << 32) - 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--field-raster", required=True)
    parser.add_argument("--value-raster", required=True)
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--artifact-dir")
    parser.add_argument("--counts-csv")
    parser.add_argument("--metadata-json")
    parser.add_argument("--request-json")
    parser.add_argument("--field-nodata", type=int, default=0)
    parser.add_argument("--value-nodata", type=int, default=0)
    parser.add_argument("--chunk-rows", type=int, default=1024)
    parser.add_argument("--include-value-nodata", action="store_true")
    return parser


def artifact_dir_path(explicit: str | None) -> Path:
    value = (explicit or os.environ.get("GOET_ARTIFACT_DIR", "")).strip()
    if not value:
        raise ValueError("artifact dir is required")
    path = Path(value)
    path.mkdir(parents=True, exist_ok=True)
    return path


def worker_output_path() -> Path:
    output_json = os.environ.get("GOET_OUTPUT_JSON", "").strip()
    if not output_json:
        raise ValueError("GOET_OUTPUT_JSON is required")
    path = Path(output_json)
    ensure_parent_dir(path)
    return path


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent_dir(path)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def copy_if_needed(source: Path, destination: Path) -> None:
    if source.resolve() == destination.resolve():
        return
    ensure_parent_dir(destination)
    shutil.copy2(source, destination)


def wait_for_input(path: Path, timeout_seconds: int = 120) -> None:
    deadline = time.monotonic() + timeout_seconds
    while not path.exists():
        if time.monotonic() >= deadline:
            raise FileNotFoundError(f"timed out waiting for raster: {path}")
        time.sleep(1)


def open_raster(path: Path, label: str) -> gdal.Dataset:
    wait_for_input(path)
    dataset = gdal.Open(str(path), gdal.GA_ReadOnly)
    if dataset is None:
        raise ValueError(f"failed to open {label}: {path}")
    if dataset.RasterCount < 1:
        raise ValueError(f"{label} has no bands: {path}")
    return dataset


def require_aligned(field_ds: gdal.Dataset, value_ds: gdal.Dataset) -> None:
    if field_ds.RasterXSize != value_ds.RasterXSize or field_ds.RasterYSize != value_ds.RasterYSize:
        raise ValueError("field and value rasters have different dimensions")
    if tuple(field_ds.GetGeoTransform()) != tuple(value_ds.GetGeoTransform()):
        raise ValueError("field and value rasters have different geotransforms")
    if field_ds.GetProjectionRef() != value_ds.GetProjectionRef():
        raise ValueError("field and value rasters have different projections")


def unsigned_uint32_values(array: np.ndarray, label: str) -> np.ndarray:
    if np.issubdtype(array.dtype, np.signedinteger) and np.any(array < 0):
        raise ValueError(f"{label} contains negative values")
    values = array.astype(np.uint64, copy=False)
    if values.size and int(values.max()) > UINT32_MAX:
        raise ValueError(f"{label} contains values above uint32 range")
    return values


def update_counts(
    counts: dict[int, int],
    field_array: np.ndarray,
    value_array: np.ndarray,
    field_nodata: int,
    value_nodata: int,
    include_value_nodata: bool,
) -> tuple[int, int, int]:
    fields = unsigned_uint32_values(field_array, "field raster")
    values = unsigned_uint32_values(value_array, "value raster")

    field_valid = fields != np.uint64(field_nodata)
    skipped_field = int(fields.size - np.count_nonzero(field_valid))
    if include_value_nodata:
        valid = field_valid
        skipped_value = 0
    else:
        value_valid = values != np.uint64(value_nodata)
        valid = field_valid & value_valid
        skipped_value = int(np.count_nonzero(field_valid & ~value_valid))

    if not np.any(valid):
        return 0, skipped_field, skipped_value

    keys = (fields[valid] << np.uint64(32)) | values[valid]
    unique_keys, unique_counts = np.unique(keys, return_counts=True)
    for key, count in zip(unique_keys.tolist(), unique_counts.tolist(), strict=True):
        counts[int(key)] = counts.get(int(key), 0) + int(count)
    return int(np.count_nonzero(valid)), skipped_field, skipped_value


def write_counts_csv(path: Path, counts: dict[int, int]) -> None:
    ensure_parent_dir(path)
    rows = [
        (key >> 32, key & UINT32_MAX, count)
        for key, count in counts.items()
    ]
    rows.sort(key=lambda row: (row[0], row[1]))

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["field_id", "crop_id", "count"])
        for field_id, crop_id, count in rows:
            writer.writerow([field_id, crop_id, count])


def pair_counts_request_payload(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "operation": "raster_pair_value_counts",
        "field_raster": args.field_raster,
        "value_raster": args.value_raster,
        "require_aligned_grid": True,
        "chunk_rows": args.chunk_rows,
        "field_dtype": "uint32",
        "value_dtype": "uint32",
        "field_nodata": args.field_nodata,
        "value_nodata": args.value_nodata,
        "include_value_nodata": args.include_value_nodata,
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.chunk_rows <= 0:
        raise ValueError("--chunk-rows must be greater than 0")
    if args.field_nodata < 0 or args.value_nodata < 0:
        raise ValueError("nodata values must be non-negative")

    artifact_dir = artifact_dir_path(args.artifact_dir)
    counts_path = artifact_dir / COUNTS_ARTIFACT
    metadata_path = artifact_dir / COUNTS_METADATA_ARTIFACT
    request_path = artifact_dir / COUNTS_REQUEST_ARTIFACT
    write_json(request_path, pair_counts_request_payload(args))

    started = time.monotonic()
    field_ds = open_raster(Path(args.field_raster), "field raster")
    value_ds = open_raster(Path(args.value_raster), "value raster")
    require_aligned(field_ds, value_ds)

    field_band = field_ds.GetRasterBand(1)
    value_band = value_ds.GetRasterBand(1)
    width = field_ds.RasterXSize
    height = field_ds.RasterYSize

    counts: dict[int, int] = {}
    valid_pixels = 0
    skipped_field_nodata = 0
    skipped_value_nodata = 0
    for y_offset in range(0, height, args.chunk_rows):
        window_height = min(args.chunk_rows, height - y_offset)
        field_array = field_band.ReadAsArray(0, y_offset, width, window_height)
        value_array = value_band.ReadAsArray(0, y_offset, width, window_height)
        if field_array is None or value_array is None:
            raise ValueError(f"failed to read raster window at row {y_offset}")
        chunk_valid, chunk_field_skip, chunk_value_skip = update_counts(
            counts,
            field_array,
            value_array,
            args.field_nodata,
            args.value_nodata,
            args.include_value_nodata,
        )
        valid_pixels += chunk_valid
        skipped_field_nodata += chunk_field_skip
        skipped_value_nodata += chunk_value_skip

    write_counts_csv(counts_path, counts)
    field_ids = {key >> 32 for key in counts}
    value_ids = {key & UINT32_MAX for key in counts}
    metadata_payload = {
        "year": args.year,
        "method": "numpy_unique_uint64_pair_key",
        "field_raster": args.field_raster,
        "value_raster": args.value_raster,
        "width": width,
        "height": height,
        "chunk_rows": args.chunk_rows,
        "field_nodata": args.field_nodata,
        "value_nodata": args.value_nodata,
        "include_value_nodata": args.include_value_nodata,
        "valid_pixels": valid_pixels,
        "skipped_field_nodata": skipped_field_nodata,
        "skipped_value_nodata": skipped_value_nodata,
        "distinct_fields": len(field_ids),
        "distinct_values": len(value_ids),
        "distinct_pairs": len(counts),
        "field_id_dtype": "uint32",
        "value_id_dtype": "uint32",
        "count_dtype": "uint64",
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    write_json(metadata_path, metadata_payload)

    shared_counts_path = Path(args.counts_csv).expanduser() if args.counts_csv else counts_path
    shared_metadata_path = Path(args.metadata_json).expanduser() if args.metadata_json else metadata_path
    shared_request_path = Path(args.request_json).expanduser() if args.request_json else request_path
    copy_if_needed(counts_path, shared_counts_path)
    copy_if_needed(metadata_path, shared_metadata_path)
    copy_if_needed(request_path, shared_request_path)

    write_json(
        worker_output_path(),
        {
            "artifacts": [
                {
                    "name": "field_crop_year_counts_csv",
                    "kind": "file",
                    "format": "csv",
                    "path": COUNTS_ARTIFACT,
                },
                {
                    "name": "field_crop_year_counts_metadata_json",
                    "kind": "file",
                    "format": "json",
                    "path": COUNTS_METADATA_ARTIFACT,
                },
                {
                    "name": "field_crop_year_counts_request_json",
                    "kind": "file",
                    "format": "json",
                    "path": COUNTS_REQUEST_ARTIFACT,
                },
            ],
            "summary": {
                "year": args.year,
                "counts_csv": shared_counts_path.as_posix(),
                "metadata_json": shared_metadata_path.as_posix(),
                "request_json": shared_request_path.as_posix(),
                "valid_pixels": valid_pixels,
                "distinct_pairs": len(counts),
                "field_id_dtype": "uint32",
            },
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
