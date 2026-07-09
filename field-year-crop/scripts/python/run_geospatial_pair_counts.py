#!/usr/bin/env python3
"""Run goet-geospatial pair counts for a synthetic field/crop-year slice."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path

from field_crop_common import ensure_parent_dir


REQUEST_ARTIFACT = "raster_pair_value_counts.request.json"
RESPONSE_ARTIFACT = "raster_pair_value_counts.response.json"
COUNTS_ARTIFACT = "field_crop_year_counts.csv"
COUNTS_METADATA_ARTIFACT = "field_crop_year_counts.metadata.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--field-raster", required=True)
    parser.add_argument("--value-raster", required=True)
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--geospatial-executable", required=True)
    parser.add_argument("--artifact-dir")
    parser.add_argument("--counts-csv")
    parser.add_argument("--metadata-json")
    parser.add_argument("--response-json")
    return parser


def artifact_dir_path(explicit: str | None) -> Path:
    value = (explicit or os.environ.get("GOET_ARTIFACT_DIR", "")).strip()
    if not value:
        raise ValueError("artifact dir is required")
    path = Path(value)
    path.mkdir(parents=True, exist_ok=True)
    return path


def copy_if_needed(source: Path, destination: Path) -> None:
    if source.resolve() == destination.resolve():
        return
    ensure_parent_dir(destination)
    shutil.copy2(source, destination)


def write_json(path: Path, payload: dict[str, object]) -> None:
    ensure_parent_dir(path)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def worker_output_path() -> Path:
    output_json = os.environ.get("GOET_OUTPUT_JSON", "").strip()
    if not output_json:
        raise ValueError("GOET_OUTPUT_JSON is required")
    path = Path(output_json)
    ensure_parent_dir(path)
    return path


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    artifact_dir = artifact_dir_path(args.artifact_dir)

    request_path = artifact_dir / REQUEST_ARTIFACT
    response_path = artifact_dir / RESPONSE_ARTIFACT
    counts_path = artifact_dir / COUNTS_ARTIFACT
    metadata_path = artifact_dir / COUNTS_METADATA_ARTIFACT

    request_payload = {
        "api_version": "goet.geospatial/v1alpha1",
        "kind": "GeospatialOperationRequest",
        "operation": "raster_pair_value_counts",
        "inputs": {
            "field_raster": {
                "path": args.field_raster,
                "band": 1,
                "nodata": 0,
            },
            "value_raster": {
                "path": args.value_raster,
                "band": 1,
                "nodata": 0,
            },
        },
        "outputs": {
            "counts_csv": COUNTS_ARTIFACT,
            "metadata_json": COUNTS_METADATA_ARTIFACT,
        },
        "options": {
            "require_aligned_grid": True,
            "chunk_rows": 1024,
            "field_dtype": "uint16",
            "value_dtype": "uint16",
        },
    }
    write_json(request_path, request_payload)

    subprocess.run(
        [args.geospatial_executable, "--request", str(request_path), "--response", str(response_path)],
        check=True,
    )

    if not counts_path.exists():
        raise FileNotFoundError(f"missing counts csv artifact: {counts_path}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"missing metadata json artifact: {metadata_path}")
    if not response_path.exists():
        raise FileNotFoundError(f"missing operation response json artifact: {response_path}")

    shared_counts_path = Path(args.counts_csv).expanduser() if args.counts_csv else counts_path
    shared_metadata_path = Path(args.metadata_json).expanduser() if args.metadata_json else metadata_path
    shared_response_path = Path(args.response_json).expanduser() if args.response_json else response_path
    copy_if_needed(counts_path, shared_counts_path)
    copy_if_needed(metadata_path, shared_metadata_path)
    copy_if_needed(response_path, shared_response_path)

    output_payload = {
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
                "name": "raster_pair_value_counts_response_json",
                "kind": "file",
                "format": "json",
                "path": RESPONSE_ARTIFACT,
            },
        ],
        "summary": {
            "year": args.year,
            "field_raster": args.field_raster,
            "value_raster": args.value_raster,
            "counts_csv": shared_counts_path.as_posix(),
            "metadata_json": shared_metadata_path.as_posix(),
            "response_json": shared_response_path.as_posix(),
        },
    }
    write_json(worker_output_path(), output_payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
