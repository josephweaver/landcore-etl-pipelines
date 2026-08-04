#!/usr/bin/env python3
"""Write a deterministic Google Drive publish plan without uploading files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


DEFAULT_GDRIVE_FOLDER_ID = "1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4"
DEFAULT_GDRIVE_FOLDER_URL = (
    "https://drive.google.com/drive/folders/1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4?usp=drive_link"
)
DEFAULT_TARGET_DRIVE_PATH = "Data/ETL/tile-field-year-crop"


def load_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("delivery manifest must be a JSON object")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delivery-manifest", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--target-folder-id", default=DEFAULT_GDRIVE_FOLDER_ID)
    parser.add_argument("--target-folder-url", default=DEFAULT_GDRIVE_FOLDER_URL)
    parser.add_argument("--target-drive-path", default=DEFAULT_TARGET_DRIVE_PATH)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest_path = Path(args.delivery_manifest)
    manifest = load_manifest(manifest_path)
    objects = []
    for output in manifest.get("outputs", []):
        path = output["path"]
        objects.append(
            {
                "source_path": path,
                "target_path": path,
                "size_bytes": output["size_bytes"],
                "sha256": output["sha256"],
                "action": "copy",
            }
        )
    payload = {
        "schema": "landcore/tile-field-year-crop-gdrive-publish-plan/v1",
        "target_folder_id": args.target_folder_id,
        "target_folder_url": args.target_folder_url,
        "target_drive_path": args.target_drive_path.strip("/"),
        "source_delivery_manifest": manifest_path.name,
        "objects": objects,
    }
    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
