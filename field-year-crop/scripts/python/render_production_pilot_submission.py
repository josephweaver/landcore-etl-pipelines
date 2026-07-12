#!/usr/bin/env python3
"""Render the production-pilot canonical workflow for a concrete HPCC run."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


TILE_RE = re.compile(r"h\d{2}v\d{2}")
CDL_YEAR_MIN = 2008
CDL_YEAR_MAX = 2023


def split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_years(value: str) -> list[int]:
    years = [int(item) for item in split_csv(value)]
    if not years:
        raise ValueError("at least one year is required")
    out_of_range = [year for year in years if year < CDL_YEAR_MIN or year > CDL_YEAR_MAX]
    if out_of_range:
        raise ValueError(f"CDL years must be {CDL_YEAR_MIN}-{CDL_YEAR_MAX}: {out_of_range}")
    return years


def parse_tiles(value: str) -> list[str]:
    tiles = split_csv(value)
    if not tiles:
        raise ValueError("at least one tile is required")
    for tile in tiles:
        if not TILE_RE.fullmatch(tile):
            raise ValueError(f"invalid tile id: {tile}")
    return tiles


def load_project_tiles(project_json: Path) -> set[str]:
    payload = json.loads(project_json.read_text(encoding="utf-8"))
    tiles = payload.get("tiles_of_interest")
    if not isinstance(tiles, list):
        raise ValueError(f"project tiles_of_interest must be a list: {project_json}")
    return {str(tile) for tile in tiles}


def render(
    template: Path,
    output: Path,
    project_json: Path,
    years: list[int],
    tiles: list[str],
    hpcc_scratch_root: str,
    landcore_data_root: str,
    product_root: str,
    delivery_root: str,
    publication_mode: str,
    production_run_id: str,
    gdrive_remote: str,
    gdrive_delivery_base_path: str,
    gorc_commit: str,
    geospatial_executable: str,
) -> None:
    allowed_tiles = load_project_tiles(project_json)
    unknown_tiles = [tile for tile in tiles if tile not in allowed_tiles]
    if unknown_tiles:
        raise ValueError(f"pilot tiles are not in land-core.project.json tiles_of_interest: {unknown_tiles}")

    workflow = json.loads(template.read_text(encoding="utf-8"))
    drive_base = gdrive_delivery_base_path.strip("/")
    variables: dict[str, Any] = workflow.setdefault("variables", {})
    variables["years"] = years
    variables["years_csv"] = ",".join(str(year) for year in years)
    variables["cdl_year_min"] = CDL_YEAR_MIN
    variables["cdl_year_max"] = CDL_YEAR_MAX
    variables["tiles"] = tiles
    variables["tiles_csv"] = ",".join(tiles)
    variables["hpcc_scratch_root"] = hpcc_scratch_root.rstrip("/")
    variables["landcore_data_root"] = landcore_data_root.rstrip("/")
    variables["product_root"] = product_root.rstrip("/")
    variables["delivery_root"] = delivery_root.rstrip("/")
    variables["publication_mode"] = publication_mode
    variables["production_run_id"] = production_run_id
    variables["gdrive_remote"] = gdrive_remote
    variables["gdrive_delivery_base_path"] = drive_base
    variables["gorc_commit"] = gorc_commit
    variables["geospatial_executable"] = geospatial_executable
    variables["year_tile_pairs"] = {
        "$type": "list",
        "$call": "list.crossproduct",
        "$args": [{"$ref": "years"}, {"$ref": "tiles"}],
    }
    outputs = workflow.setdefault("data", {}).setdefault("outputs", {})
    package_output = outputs.get("field_year_crop_delivery_package", {})
    location = package_output.setdefault("binding", {}).setdefault("location", {})
    location["remote"] = gdrive_remote
    location["drive_path"] = f"{drive_base}/${{run_id}}/tile-field-year-crop-delivery.zip"

    steps = workflow.get("steps", [])
    if publication_mode == "plan_only":
        workflow["steps"] = [step for step in steps if step.get("id") != "publish-delivery"]

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(workflow, indent=2) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--template", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--project-json", required=True)
    parser.add_argument("--years", default="2010")
    parser.add_argument("--tiles", default="h18v07,h23v08")
    parser.add_argument("--hpcc-scratch-root", default="/mnt/scratch/weave151/etl")
    parser.add_argument("--landcore-data-root", default="/mnt/scratch/weave151/data")
    parser.add_argument("--product-root", required=True)
    parser.add_argument("--delivery-root", required=True)
    parser.add_argument("--publication-mode", default="plan_only", choices=["plan_only", "commit_gdrive"])
    parser.add_argument("--production-run-id", required=True)
    parser.add_argument("--gdrive-remote", default="gdrive")
    parser.add_argument("--gdrive-delivery-base-path", default="Data/ETL/tile-field-year-crop")
    parser.add_argument("--gorc-commit", default="unknown")
    parser.add_argument("--geospatial-executable", default="/goetl/goet-geospatial")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    render(
        template=Path(args.template),
        output=Path(args.output),
        project_json=Path(args.project_json),
        years=parse_years(args.years),
        tiles=parse_tiles(args.tiles),
        hpcc_scratch_root=args.hpcc_scratch_root,
        landcore_data_root=args.landcore_data_root,
        product_root=args.product_root,
        delivery_root=args.delivery_root,
        publication_mode=args.publication_mode,
        production_run_id=args.production_run_id,
        gdrive_remote=args.gdrive_remote,
        gdrive_delivery_base_path=args.gdrive_delivery_base_path,
        gorc_commit=args.gorc_commit,
        geospatial_executable=args.geospatial_executable,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
