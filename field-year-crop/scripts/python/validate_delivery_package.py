#!/usr/bin/env python3
"""Validate a field-crop-year delivery package and publish plan."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from field_crop_common import read_csv_rows, safe_int, sha256_file
from merge_field_crop_year_outputs import load_work_units


EXPECTED_FOLDER_ID = "1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4"
EXPECTED_DRIVE_PATH = "Data/ETL/tile-field-year-crop"
PRIVATE_INPUT_SUFFIXES = (".tif", ".tiff", ".img", ".bil", ".hdr")


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def require_no_duplicate_keys(path: Path, fields: tuple[str, ...]) -> int:
    rows = read_csv_rows(path)
    seen: set[tuple[str, ...]] = set()
    for index, row in enumerate(rows, start=1):
        key = tuple(str(row.get(field, "")).strip() for field in fields)
        if key in seen:
            raise ValueError(f"duplicate key in {path} row {index}: {key}")
        seen.add(key)
    return len(rows)


def require_hashes(root: Path, manifest: dict[str, Any]) -> None:
    for output in manifest.get("outputs", []):
        path = root / output["path"]
        if not path.exists():
            raise FileNotFoundError(f"manifest output missing: {path}")
        if path.stat().st_size != output["size_bytes"]:
            raise ValueError(f"manifest size mismatch: {output['path']}")
        if sha256_file(path) != output["sha256"]:
            raise ValueError(f"manifest sha256 mismatch: {output['path']}")


def require_pair_count_request(path: Path) -> None:
    payload = load_json(path)
    require_aligned = payload.get("require_aligned_grid")
    if require_aligned is None and isinstance(payload.get("options"), dict):
        require_aligned = payload["options"].get("require_aligned_grid")
    if require_aligned is not True:
        raise ValueError(f"pair-count request did not require aligned grid: {path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delivery-root", required=True)
    parser.add_argument("--delivery-manifest", required=True)
    parser.add_argument("--gdrive-publish-plan", required=True)
    parser.add_argument("--work-units-json", required=True)
    parser.add_argument("--output-json", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path(args.delivery_root)
    manifest = load_json(Path(args.delivery_manifest))
    publish_plan = load_json(Path(args.gdrive_publish_plan))
    units = load_work_units(Path(args.work_units_json))

    allowed_states = set(manifest.get("states_of_interest", []))
    allowed_tiles = set(manifest.get("tiles_of_interest", []))
    if publish_plan.get("target_folder_id") != EXPECTED_FOLDER_ID:
        raise ValueError("unexpected Google Drive target folder")
    if publish_plan.get("target_drive_path") != EXPECTED_DRIVE_PATH:
        raise ValueError("unexpected Google Drive target drive path")
    if manifest.get("publication", {}).get("folder_id") != EXPECTED_FOLDER_ID:
        raise ValueError("manifest publication folder mismatch")

    require_hashes(root, manifest)
    output_paths = {output["path"] for output in manifest.get("outputs", [])}
    for planned in publish_plan.get("objects", []):
        if planned.get("source_path") not in output_paths:
            raise ValueError(f"publish plan references non-manifest file: {planned.get('source_path')}")
    for path in output_paths:
        if path.lower().endswith(PRIVATE_INPUT_SUFFIXES):
            raise ValueError(f"private raster-like input copied into delivery package: {path}")

    counts_rows = require_no_duplicate_keys(root / "field_crop_year_counts_all.csv", ("field_id", "crop_id", "year", "tile"))
    summary_rows = require_no_duplicate_keys(root / "field_crop_year_summary_all.csv", ("field_id", "crop_id", "year", "tile"))

    work_unit_keys: set[tuple[int, str]] = set()
    for index, unit in enumerate(units, start=1):
        year = safe_int(unit.get("year"), f"work unit {index} year")
        tile = str(unit.get("tile", "")).strip()
        state = str(unit.get("state", "")).strip()
        if tile not in allowed_tiles:
            raise ValueError(f"work unit {index} tile is not allowed: {tile}")
        if state and state not in allowed_states:
            raise ValueError(f"work unit {index} state is not allowed: {state}")
        work_unit_keys.add((year, tile))
        validation = load_json(Path(str(unit["validation_json"])))
        if validation.get("status") != "passed":
            raise ValueError(f"work unit {index} validation did not pass")
        for key in ("raster_info_json", "alignment_metadata_json", "pair_counts_metadata_json"):
            path = Path(str(unit.get(key, "")))
            if not path.exists():
                raise FileNotFoundError(f"work unit {index} missing {key}: {path}")
            load_json(path)
        require_pair_count_request(Path(str(unit["pair_counts_request_json"])))

    manifest_keys = {(safe_int(unit.get("year"), "manifest work unit year"), str(unit.get("tile", "")).strip()) for unit in manifest.get("work_units", [])}
    if work_unit_keys != manifest_keys:
        raise ValueError("manifest work units do not match expected work units")

    result = {
        "schema": "landcore/field-crop-year-delivery-validation/v1",
        "status": "passed",
        "work_unit_count": len(units),
        "counts_row_count": counts_rows,
        "summary_row_count": summary_rows,
        "publish_object_count": len(publish_plan.get("objects", [])),
    }
    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
