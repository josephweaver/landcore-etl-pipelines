#!/usr/bin/env python3
"""Validate the local real field-crop-year product."""

from __future__ import annotations

import argparse
import json
import os
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Any

from field_crop_common import ensure_parent_dir, read_csv_rows, safe_float, safe_int


VALIDATION_ARTIFACT = "validation/field_crop_year_validation_2010.json"
SHARE_TOLERANCE = 0.00001


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--counts-csv", required=True)
    parser.add_argument("--summary-csv", required=True)
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--raster-info-json", required=True)
    parser.add_argument("--alignment-metadata-json", required=True)
    parser.add_argument("--counts-metadata-json", required=True)
    parser.add_argument("--summary-metadata-json", required=True)
    parser.add_argument("--input-discovery-json", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--artifact-dir")
    return parser


def artifact_dir_path(explicit: str | None) -> Path | None:
    value = (explicit or os.environ.get("GOET_ARTIFACT_DIR", "")).strip()
    if not value:
        return None
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


def require_nonempty_file(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"missing {label}: {path}")
    if path.stat().st_size <= 0:
        raise ValueError(f"{label} is empty: {path}")


def load_json(path: Path, label: str) -> dict[str, Any]:
    require_nonempty_file(path, label)
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def validate_counts(rows: list[dict[str, str]]) -> tuple[int, int, dict[int, dict[int, int]]]:
    if not rows:
        raise ValueError("counts CSV has no data rows")

    max_field_id = 0
    total_pixels = 0
    by_field: dict[int, dict[int, int]] = defaultdict(dict)
    for row_index, row in enumerate(rows, start=1):
        field_id = safe_int(row.get("field_id"), f"counts row {row_index} field_id")
        crop_id = safe_int(row.get("crop_id"), f"counts row {row_index} crop_id")
        count = safe_int(row.get("count"), f"counts row {row_index} count")
        if field_id <= 0:
            raise ValueError(f"counts row {row_index} field_id must be positive")
        if crop_id < 0:
            raise ValueError(f"counts row {row_index} crop_id must be non-negative")
        if count <= 0:
            raise ValueError(f"counts row {row_index} count must be positive")
        by_field[field_id][crop_id] = by_field[field_id].get(crop_id, 0) + count
        max_field_id = max(max_field_id, field_id)
        total_pixels += count
    return max_field_id, total_pixels, by_field


def validate_summary(
    rows: list[dict[str, str]],
    year: int,
    counts_by_field: dict[int, dict[int, int]],
) -> None:
    if not rows:
        raise ValueError("summary CSV has no data rows")

    shares_by_field: dict[int, float] = defaultdict(float)
    dominant_true_count: dict[int, int] = defaultdict(int)
    for row_index, row in enumerate(rows, start=1):
        field_id = safe_int(row.get("field_id"), f"summary row {row_index} field_id")
        row_year = safe_int(row.get("year"), f"summary row {row_index} year")
        crop_id = safe_int(row.get("crop_id"), f"summary row {row_index} crop_id")
        pixel_count = safe_int(row.get("pixel_count"), f"summary row {row_index} pixel_count")
        total_pixels = safe_int(row.get("total_field_pixels"), f"summary row {row_index} total_field_pixels")
        share = safe_float(row.get("share"), f"summary row {row_index} share")
        dominant_crop_id = safe_int(
            row.get("dominant_crop_id"), f"summary row {row_index} dominant_crop_id"
        )
        dominant_share = safe_float(
            row.get("dominant_crop_share"), f"summary row {row_index} dominant_crop_share"
        )
        is_dominant = str(row.get("is_dominant", "")).strip().lower()

        if row_year != year:
            raise ValueError(f"summary row {row_index} year {row_year} != {year}")
        if pixel_count <= 0 or total_pixels <= 0:
            raise ValueError(f"summary row {row_index} pixel counts must be positive")
        if not 0 <= share <= 1 or not 0 <= dominant_share <= 1:
            raise ValueError(f"summary row {row_index} share values must be between 0 and 1")
        if is_dominant not in {"true", "false"}:
            raise ValueError(f"summary row {row_index} is_dominant must be true or false")
        if field_id not in counts_by_field or crop_id not in counts_by_field[field_id]:
            raise ValueError(f"summary row {row_index} has no matching count row")
        if counts_by_field[field_id][crop_id] != pixel_count:
            raise ValueError(f"summary row {row_index} pixel_count does not match counts CSV")

        expected_total = sum(counts_by_field[field_id].values())
        if total_pixels != expected_total:
            raise ValueError(f"summary row {row_index} total_field_pixels mismatch")
        expected_dominant = min(
            counts_by_field[field_id],
            key=lambda candidate: (-counts_by_field[field_id][candidate], candidate),
        )
        if dominant_crop_id != expected_dominant:
            raise ValueError(f"summary row {row_index} dominant crop is not deterministic")
        if is_dominant == "true":
            dominant_true_count[field_id] += 1
            if crop_id != expected_dominant:
                raise ValueError(f"summary row {row_index} marks a non-dominant crop as dominant")
        shares_by_field[field_id] += share

    for field_id, share_sum in shares_by_field.items():
        if abs(share_sum - 1.0) > SHARE_TOLERANCE:
            raise ValueError(f"shares for field_id={field_id} sum to {share_sum}, not 1.0")
        if dominant_true_count[field_id] != 1:
            raise ValueError(f"field_id={field_id} must have exactly one dominant row")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    counts_csv = Path(args.counts_csv)
    summary_csv = Path(args.summary_csv)
    metadata_paths = [
        Path(args.raster_info_json),
        Path(args.alignment_metadata_json),
        Path(args.counts_metadata_json),
        Path(args.summary_metadata_json),
        Path(args.input_discovery_json),
    ]
    for path in [counts_csv, summary_csv, *metadata_paths]:
        require_nonempty_file(path, path.name)

    counts_rows = read_csv_rows(counts_csv)
    summary_rows = read_csv_rows(summary_csv)
    if len(summary_rows) != len(counts_rows):
        raise ValueError("summary row count equals counts row count check failed")

    max_field_id, total_counted_pixels, counts_by_field = validate_counts(counts_rows)
    validate_summary(summary_rows, args.year, counts_by_field)
    loaded_metadata = [load_json(path, path.name) for path in metadata_paths]

    validation_payload = {
        "status": "passed",
        "year": args.year,
        "counts_row_count": len(counts_rows),
        "summary_row_count": len(summary_rows),
        "distinct_field_count": len(counts_by_field),
        "total_counted_pixels": total_counted_pixels,
        "max_field_id": max_field_id,
        "field_id_dtype": "uint32",
        "metadata_files": [str(path) for path in metadata_paths],
        "metadata_json_objects_loaded": len(loaded_metadata),
    }
    output_json = Path(args.output_json)
    write_json(output_json, validation_payload)

    artifact_dir = artifact_dir_path(args.artifact_dir)
    artifact_path = None
    if artifact_dir is not None:
        artifact_path = artifact_dir / VALIDATION_ARTIFACT
        if artifact_path.resolve() != output_json.resolve():
            ensure_parent_dir(artifact_path)
            shutil.copy2(output_json, artifact_path)

    write_json(
        worker_output_path(),
        {
            "artifacts": [
                {
                    "name": "field_crop_year_validation_json",
                    "kind": "file",
                    "format": "json",
                    "path": VALIDATION_ARTIFACT,
                }
            ],
            "summary": validation_payload,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
