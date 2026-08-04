#!/usr/bin/env python3
"""Package production pilot field-crop-year outputs into a delivery directory."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import zipfile
from pathlib import Path

import merge_field_crop_year_outputs
import validate_delivery_package
import write_delivery_manifest
import write_gdrive_publish_plan


def split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_years(value: str) -> list[int]:
    years = []
    for item in split_csv(value):
        years.append(int(item))
    if not years:
        raise ValueError("at least one year is required")
    return years


def parse_tiles(value: str) -> list[str]:
    tiles = split_csv(value)
    if not tiles:
        raise ValueError("at least one tile is required")
    return tiles


def wait_for_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(path)
    if path.stat().st_size <= 0:
        raise ValueError(f"file is empty: {path}")


def unit_paths(product_root: Path, year: int, tile: str) -> dict[str, str | int]:
    unit_root = product_root / tile / str(year)
    return {
        "year": year,
        "tile": tile,
        "state": "",
        "counts_csv": str(unit_root / "counts" / f"field_crop_counts_{year}_{tile}.csv"),
        "summary_csv": str(unit_root / "summary" / f"field_crop_year_summary_{year}_{tile}.csv"),
        "raster_info_json": str(unit_root / "metadata" / "raster_info.json"),
        "alignment_metadata_json": str(unit_root / "aligned" / f"cdl_{year}_on_{tile}_grid.metadata.json"),
        "pair_counts_request_json": str(unit_root / "counts" / f"field_crop_counts_{year}_{tile}.request.json"),
        "pair_counts_metadata_json": str(unit_root / "counts" / f"field_crop_counts_{year}_{tile}.metadata.json"),
        "validation_json": str(unit_root / "validation" / f"field_crop_year_validation_{year}_{tile}.json"),
    }


def copy_report_inputs(delivery_root: Path, units: list[dict[str, str | int]]) -> None:
    for unit in units:
        unit_dir = delivery_root / "units" / f"{unit['year']}-{unit['tile']}"
        unit_dir.mkdir(parents=True, exist_ok=True)
        for key in (
            "counts_csv",
            "summary_csv",
            "raster_info_json",
            "alignment_metadata_json",
            "pair_counts_request_json",
            "pair_counts_metadata_json",
            "validation_json",
        ):
            source = Path(str(unit[key]))
            wait_for_file(source)
            shutil.copy2(source, unit_dir / source.name)


def artifact_dir() -> Path | None:
    value = os.environ.get("GOET_ARTIFACT_DIR", "").strip()
    if not value:
        return None
    path = Path(value)
    path.mkdir(parents=True, exist_ok=True)
    return path


def copy_package_artifacts(delivery_root: Path) -> None:
    target_root = artifact_dir()
    if target_root is None:
        return
    for name in (
        "delivery_manifest.json",
        "gdrive_publish_plan.json",
        "delivery_validation.json",
        "tile-field-year-crop-delivery.zip",
    ):
        source = delivery_root / name
        wait_for_file(source)
        shutil.copy2(source, target_root / name)


def create_delivery_archive(delivery_root: Path) -> Path:
    archive_name = "tile-field-year-crop-delivery.zip"
    archive_path = delivery_root / archive_name
    temp_path = delivery_root.parent / f".{delivery_root.name}-{archive_name}.tmp"
    if temp_path.exists():
        temp_path.unlink()
    with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(delivery_root.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(delivery_root).as_posix()
            if relative == archive_name:
                continue
            archive.write(path, relative)
    temp_path.replace(archive_path)
    wait_for_file(archive_path)
    return archive_path


def write_worker_output(delivery_root: Path, validation_json: Path, units: list[dict[str, str | int]]) -> None:
    output_json = os.environ.get("GOET_OUTPUT_JSON", "").strip()
    if not output_json:
        return
    payload = {
        "artifacts": [
            {
                "name": "field_year_crop_delivery_manifest",
                "kind": "file",
                "format": "json",
                "path": "delivery_manifest.json",
            },
            {
                "name": "field_year_crop_gdrive_publish_plan",
                "kind": "file",
                "format": "json",
                "path": "gdrive_publish_plan.json",
            },
            {
                "name": "field_year_crop_delivery_validation",
                "kind": "file",
                "format": "json",
                "path": "delivery_validation.json",
            },
            {
                "name": "field_year_crop_delivery_package",
                "kind": "file",
                "format": "zip",
                "content_type": "application/zip",
                "path": "tile-field-year-crop-delivery.zip",
            },
        ],
        "summary": {
            "delivery_root": str(delivery_root),
            "validation_json": str(validation_json),
            "work_unit_count": len(units),
        },
    }
    path = Path(output_json)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", required=True, help="Comma-separated years.")
    parser.add_argument("--tiles", required=True, help="Comma-separated tiles.")
    parser.add_argument("--product-root", required=True)
    parser.add_argument("--delivery-root", required=True)
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--project-json", required=True)
    parser.add_argument("--gorc-commit", default="unknown")
    parser.add_argument("--production-run-id", required=True)
    parser.add_argument("--publication-mode", default="plan_only", choices=["plan_only", "commit_gdrive"])
    parser.add_argument("--gdrive-target-drive-path", default="Data/ETL/tile-field-year-crop")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    years = parse_years(args.years)
    tiles = parse_tiles(args.tiles)
    product_root = Path(args.product_root)
    delivery_root = Path(args.delivery_root)
    delivery_root.mkdir(parents=True, exist_ok=True)
    for stale_name in (
        "delivery_manifest.json",
        "gdrive_publish_plan.json",
        "delivery_validation.json",
        "tile-field-year-crop-delivery.zip",
    ):
        stale_path = delivery_root / stale_name
        if stale_path.exists():
            stale_path.unlink()

    units = [unit_paths(product_root, year, tile) for year in years for tile in tiles]
    work_units_json = delivery_root / "work_units.json"
    work_units_json.write_text(json.dumps({"work_units": units}, indent=2) + "\n", encoding="utf-8")

    copy_report_inputs(delivery_root, units)

    merge_field_crop_year_outputs.main(
        [
            "--work-units-json",
            str(work_units_json),
            "--output-dir",
            str(delivery_root),
        ]
    )
    manifest_json = delivery_root / "delivery_manifest.json"
    write_delivery_manifest.main(
        [
            "--delivery-root",
            str(delivery_root),
            "--work-units-json",
            str(work_units_json),
            "--output-json",
            str(manifest_json),
            "--workflow",
            args.workflow,
            "--project-json",
            args.project_json,
            "--gorc-commit",
            args.gorc_commit,
            "--production-run-id",
            args.production_run_id,
        ]
    )
    publish_plan_json = delivery_root / "gdrive_publish_plan.json"
    write_gdrive_publish_plan.main(
        [
            "--delivery-manifest",
            str(manifest_json),
            "--output-json",
            str(publish_plan_json),
            "--target-drive-path",
            args.gdrive_target_drive_path,
        ]
    )
    validation_json = delivery_root / "delivery_validation.json"
    validate_delivery_package.main(
        [
            "--delivery-root",
            str(delivery_root),
            "--delivery-manifest",
            str(manifest_json),
            "--gdrive-publish-plan",
            str(publish_plan_json),
            "--work-units-json",
            str(work_units_json),
            "--output-json",
            str(validation_json),
        ]
    )

    create_delivery_archive(delivery_root)
    copy_package_artifacts(delivery_root)
    write_worker_output(delivery_root, validation_json, units)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
