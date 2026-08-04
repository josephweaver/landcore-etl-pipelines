#!/usr/bin/env python3
"""Write a delivery manifest for a field-crop-year package."""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from field_crop_common import sha256_file
from merge_field_crop_year_outputs import load_work_units


DEFAULT_GDRIVE_FOLDER_ID = "1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4"
DEFAULT_GDRIVE_FOLDER_URL = (
    "https://drive.google.com/drive/folders/1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4?usp=drive_link"
)


def utc_now() -> str:
    epoch = os.environ.get("SOURCE_DATE_EPOCH", "").strip()
    if epoch:
        return datetime.fromtimestamp(int(epoch), tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def rel_path(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def list_outputs(root: Path, excluded: set[str]) -> list[dict[str, Any]]:
    outputs: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = rel_path(path, root)
        if relative in excluded:
            continue
        outputs.append(
            {
                "path": relative,
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return outputs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delivery-root", required=True)
    parser.add_argument("--work-units-json", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--project-json", default="land-core.project.json")
    parser.add_argument("--gorc-commit", default="unknown")
    parser.add_argument("--production-run-id", default="production-dry-run")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    delivery_root = Path(args.delivery_root)
    output_json = Path(args.output_json)
    project = load_json(Path(args.project_json))
    units = load_work_units(Path(args.work_units_json))

    validation = {"status": "passed", "work_units": []}
    for unit in units:
        validation_json = unit.get("validation_json")
        status = "unknown"
        if validation_json:
            payload = load_json(Path(str(validation_json)))
            status = str(payload.get("status", "unknown"))
            if status != "passed":
                validation["status"] = "failed"
        validation["work_units"].append(
            {
                "year": unit.get("year"),
                "tile": unit.get("tile"),
                "state": unit.get("state"),
                "validation_status": status,
            }
        )

    output_relative = rel_path(output_json, delivery_root) if output_json.is_relative_to(delivery_root) else output_json.name
    outputs = list_outputs(delivery_root, {output_relative, "gdrive_publish_plan.json"})
    manifest = {
        "schema": "landcore/field-crop-year-delivery/v1",
        "created_at": utc_now(),
        "production_run_id": args.production_run_id,
        "gorc_repository": "https://github.com/josephweaver/go-etl",
        "gorc_commit": args.gorc_commit,
        "workflow": args.workflow,
        "landcore_repository": project["source_repositories"]["landcore_etl_pipelines"],
        "landcore_data_catalog_repository": project["source_repositories"]["landcore_data_catalog"],
        "yanroy_release_drive_file_id": project["google_drive_endpoints"]["yanroy_release_file_id"],
        "tile_field_year_crop_publish_drive_folder_id": DEFAULT_GDRIVE_FOLDER_ID,
        "tile_field_year_crop_publish_drive_url": DEFAULT_GDRIVE_FOLDER_URL,
        "states_of_interest": project["states_of_interest"],
        "tiles_of_interest": project["tiles_of_interest"],
        "work_units": [
            {
                "year": unit.get("year"),
                "tile": unit.get("tile"),
                "state": unit.get("state"),
                "counts_csv": unit.get("counts_csv"),
                "summary_csv": unit.get("summary_csv"),
                "validation_json": unit.get("validation_json"),
            }
            for unit in units
        ],
        "outputs": outputs,
        "publication": {
            "target": "google_drive_folder",
            "status": "planned",
            "folder_id": DEFAULT_GDRIVE_FOLDER_ID,
            "uploaded_objects": [],
        },
        "validation": validation,
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
