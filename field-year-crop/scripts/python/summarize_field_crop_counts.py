#!/usr/bin/env python3
"""Summarize field/crop count rows into field-year dominant crop rows."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from collections import defaultdict
from pathlib import Path

from field_crop_common import read_csv_rows, safe_int, sha256_file, write_csv_rows


OUTPUT_FIELDS = [
    "field_id",
    "year",
    "crop_id",
    "pixel_count",
    "total_field_pixels",
    "share",
    "is_dominant",
    "dominant_crop_id",
    "dominant_crop_share",
]

SUMMARY_ARTIFACT_CSV = "field_crop_year_summary.csv"
SUMMARY_ARTIFACT_METADATA = "field_crop_year_summary.metadata.json"


def format_share(value: float) -> str:
    return f"{value:.6f}"


def summarize_rows(rows: list[dict[str, str]], year: int) -> list[dict[str, str]]:
    field_crop_counts: dict[int, dict[int, int]] = defaultdict(dict)
    field_totals: dict[int, int] = defaultdict(int)

    for row_index, row in enumerate(rows, start=1):
        field_id = safe_int(row.get("field_id"), f"row {row_index} field_id")
        crop_id = safe_int(row.get("crop_id"), f"row {row_index} crop_id")
        count = safe_int(row.get("count"), f"row {row_index} count")
        if field_id < 0 or crop_id < 0 or count < 0:
            raise ValueError("field_id, crop_id, and count must be non-negative integers")
        previous = field_crop_counts[field_id].get(crop_id, 0)
        field_crop_counts[field_id][crop_id] = previous + count
        field_totals[field_id] += count

    output_rows: list[dict[str, str]] = []
    for field_id in sorted(field_crop_counts):
        crop_counts = field_crop_counts[field_id]
        total_field_pixels = field_totals[field_id]
        dominant_crop_id = min(
            crop_counts,
            key=lambda crop_id: (-crop_counts[crop_id], crop_id),
        )
        dominant_crop_share = (
            crop_counts[dominant_crop_id] / total_field_pixels if total_field_pixels else 0.0
        )
        for crop_id in sorted(crop_counts):
            pixel_count = crop_counts[crop_id]
            share = pixel_count / total_field_pixels if total_field_pixels else 0.0
            output_rows.append(
                {
                    "field_id": str(field_id),
                    "year": str(year),
                    "crop_id": str(crop_id),
                    "pixel_count": str(pixel_count),
                    "total_field_pixels": str(total_field_pixels),
                    "share": format_share(share),
                    "is_dominant": "true" if crop_id == dominant_crop_id else "false",
                    "dominant_crop_id": str(dominant_crop_id),
                    "dominant_crop_share": format_share(dominant_crop_share),
                }
            )

    return output_rows


def write_metadata(
    metadata_path: str | Path,
    counts_csv: str | Path,
    output_csv: str | Path,
    year: int,
    input_row_count: int,
    output_row_count: int,
    distinct_field_count: int,
) -> None:
    metadata = {
        "input_row_count": input_row_count,
        "output_row_count": output_row_count,
        "distinct_field_count": distinct_field_count,
        "year": year,
        "input_sha256": sha256_file(counts_csv),
        "output_sha256": sha256_file(output_csv),
    }
    metadata_path = Path(metadata_path)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with metadata_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(metadata, handle, indent=2, sort_keys=True)
        handle.write("\n")


def copy_if_needed(source: Path, destination: Path) -> None:
    if source.resolve() == destination.resolve():
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def wait_for_input(path: Path, timeout_seconds: int = 120) -> None:
    deadline = time.monotonic() + timeout_seconds
    while not path.exists():
        if time.monotonic() >= deadline:
            raise FileNotFoundError(f"timed out waiting for counts csv: {path}")
        time.sleep(1)


def artifact_root() -> Path | None:
    value = os.environ.get("GOET_ARTIFACT_DIR", "").strip()
    if not value:
        return None
    return Path(value)


def write_worker_output(output_csv: Path, metadata_json: Path, year: int, input_rows: int, output_rows: int) -> None:
    output_json = os.environ.get("GOET_OUTPUT_JSON", "").strip()
    if not output_json:
        return

    artifact_dir = artifact_root()
    if artifact_dir is None:
        artifact_dir = output_csv.parent

    artifact_csv = artifact_dir / SUMMARY_ARTIFACT_CSV
    artifact_metadata = artifact_dir / SUMMARY_ARTIFACT_METADATA
    copy_if_needed(output_csv, artifact_csv)
    copy_if_needed(metadata_json, artifact_metadata)

    payload = {
        "artifacts": [
            {
                "name": "field_crop_year_summary_csv",
                "kind": "file",
                "format": "csv",
                "path": SUMMARY_ARTIFACT_CSV,
            },
            {
                "name": "field_crop_year_summary_metadata_json",
                "kind": "file",
                "format": "json",
                "path": SUMMARY_ARTIFACT_METADATA,
            },
        ],
        "summary": {
            "year": year,
            "input_row_count": input_rows,
            "output_row_count": output_rows,
            "distinct_field_count": len({row["field_id"] for row in read_csv_rows(output_csv)}),
        },
    }

    output_path = Path(output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--counts-csv", required=True)
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--metadata-json", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    wait_for_input(Path(args.counts_csv))
    rows = read_csv_rows(args.counts_csv)
    output_rows = summarize_rows(rows, args.year)
    output_csv = Path(args.output_csv)
    metadata_json = Path(args.metadata_json)
    artifact_dir = artifact_root()
    if artifact_dir is not None:
        write_csv_rows(artifact_dir / SUMMARY_ARTIFACT_CSV, OUTPUT_FIELDS, output_rows)
        write_metadata(
            artifact_dir / SUMMARY_ARTIFACT_METADATA,
            args.counts_csv,
            artifact_dir / SUMMARY_ARTIFACT_CSV,
            args.year,
            len(rows),
            len(output_rows),
            len({row["field_id"] for row in output_rows}),
        )
        copy_if_needed(artifact_dir / SUMMARY_ARTIFACT_CSV, output_csv)
        copy_if_needed(artifact_dir / SUMMARY_ARTIFACT_METADATA, metadata_json)
    else:
        write_csv_rows(output_csv, OUTPUT_FIELDS, output_rows)
        write_metadata(
            metadata_json,
            args.counts_csv,
            output_csv,
            args.year,
            len(rows),
            len(output_rows),
            len({row["field_id"] for row in output_rows}),
        )
    write_worker_output(output_csv, metadata_json, args.year, len(rows), len(output_rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
