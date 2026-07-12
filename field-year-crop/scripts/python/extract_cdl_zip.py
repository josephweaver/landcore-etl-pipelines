#!/usr/bin/env python3
"""Extract a cached CDL ZIP into the shared HPCC source directory."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from discover_raster_asset import choose_candidate, discover_candidates
from field_crop_common import ensure_parent_dir, sha256_file


METADATA_ARTIFACT = "metadata/cdl_extract_metadata.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cdl-zip", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--artifact-dir")
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


def safe_members(archive: zipfile.ZipFile, destination: Path) -> list[zipfile.ZipInfo]:
    destination_root = destination.resolve()
    members: list[zipfile.ZipInfo] = []
    for member in archive.infolist():
        member_path = destination / member.filename
        try:
            resolved = member_path.resolve()
        except FileNotFoundError:
            resolved = member_path.parent.resolve() / member_path.name
        if resolved != destination_root and destination_root not in resolved.parents:
            raise ValueError(f"ZIP member escapes output root: {member.filename}")
        members.append(member)
    return members


def discover_single_raster(asset_path: Path) -> tuple[Path, list[Path]]:
    candidates = discover_candidates(asset_path)
    selected = choose_candidate(candidates)
    if selected is None:
        candidate_text = ", ".join(str(candidate) for candidate in candidates)
        raise ValueError("could not identify a single CDL raster: " + candidate_text)
    return selected, candidates


def has_discoverable_raster(output_root: Path) -> bool:
    if not output_root.exists():
        return False
    try:
        discover_single_raster(output_root)
    except (FileNotFoundError, ValueError):
        return False
    return True


def extract_zip(cdl_zip: Path, output_root: Path) -> None:
    output_parent = output_root.parent
    output_parent.mkdir(parents=True, exist_ok=True)
    temp_root = Path(tempfile.mkdtemp(prefix=f".{output_root.name}.", dir=output_parent))
    try:
        with zipfile.ZipFile(cdl_zip) as archive:
            members = safe_members(archive, temp_root)
            archive.extractall(temp_root, members)
        discover_single_raster(temp_root)
        if output_root.is_symlink():
            output_root.unlink()
        elif output_root.exists():
            shutil.rmtree(output_root)
        os.replace(temp_root, output_root)
    except Exception:
        shutil.rmtree(temp_root, ignore_errors=True)
        raise


def artifact_descriptor(name: str, kind: str, fmt: str, path: str) -> dict[str, str]:
    return {"name": name, "kind": kind, "format": fmt, "path": path}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cdl_zip = Path(args.cdl_zip)
    output_root = Path(args.output_root)
    artifact_dir = artifact_dir_path(args.artifact_dir)
    if not cdl_zip.is_file():
        raise FileNotFoundError(f"CDL ZIP does not exist: {cdl_zip}")

    reused = has_discoverable_raster(output_root)
    if not reused:
        extract_zip(cdl_zip, output_root)

    primary_raster, candidates = discover_single_raster(output_root)
    metadata = {
        "year": args.year,
        "cdl_zip": str(cdl_zip),
        "cdl_zip_sha256": sha256_file(cdl_zip),
        "output_root": str(output_root),
        "reused_existing_extraction": reused,
        "primary_raster_path": str(primary_raster),
        "raster_candidates": [str(candidate) for candidate in candidates],
        "candidate_count": len(candidates),
    }
    write_json(artifact_dir / METADATA_ARTIFACT, metadata)
    write_json(
        worker_output_path(),
        {
            "artifacts": [
                artifact_descriptor("cdl_extract_metadata_json", "file", "json", METADATA_ARTIFACT)
            ],
            "summary": metadata,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
