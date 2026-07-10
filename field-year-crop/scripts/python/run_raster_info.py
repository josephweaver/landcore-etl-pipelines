#!/usr/bin/env python3
"""Discover real raster inputs and run goet-geospatial raster_info."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from discover_raster_asset import choose_candidate, discover_candidates
from field_crop_common import ensure_parent_dir


CDL_METADATA_ARTIFACT = "metadata/cdl_2010_raster_info.json"
YANROY_METADATA_ARTIFACT = "metadata/yanroy_h18v07_raster_info.json"
DISCOVERY_ARTIFACT = "metadata/input_discovery.json"
CDL_RESPONSE_ARTIFACT = "metadata/cdl_2010_raster_info.response.json"
YANROY_RESPONSE_ARTIFACT = "metadata/yanroy_h18v07_raster_info.response.json"


UINT16_COMPATIBLE_DTYPES = {"Byte", "UInt16"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cdl-asset-path", required=True)
    parser.add_argument("--yanroy-raster", required=True)
    parser.add_argument("--geospatial-executable", required=True)
    parser.add_argument("--artifact-dir")
    parser.add_argument("--output-root")
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


def raster_info_request(raster_name: str, raster_path: Path, metadata_path: str) -> dict[str, Any]:
    return {
        "api_version": "goet.geospatial/v1alpha1",
        "kind": "GeospatialOperationRequest",
        "operation": "raster_info",
        "inputs": {
            raster_name: {
                "path": str(raster_path),
                "band": 1,
            }
        },
        "outputs": {
            "metadata_json": metadata_path,
        },
        "options": {},
    }


def run_geospatial(
    executable: str,
    artifact_dir: Path,
    request_name: str,
    response_name: str,
    request_payload: dict[str, Any],
) -> dict[str, Any]:
    request_path = artifact_dir / "metadata" / request_name
    response_path = artifact_dir / response_name
    write_json(request_path, request_payload)

    subprocess.run(
        [executable, "--request", str(request_path), "--response", str(response_path)],
        check=True,
    )

    if not response_path.exists():
        raise FileNotFoundError(f"missing geospatial response: {response_path}")
    with response_path.open("r", encoding="utf-8") as handle:
        response_payload = json.load(handle)

    metadata_json = request_payload.get("outputs", {}).get("metadata_json")
    response_summary = response_payload.get("summary")
    if isinstance(metadata_json, str) and isinstance(response_summary, dict):
        metadata_path = artifact_dir / metadata_json
        if not metadata_path.exists() and isinstance(response_summary.get("rasters"), list):
            write_json(metadata_path, {"rasters": response_summary["rasters"]})

    return response_payload


def load_raster_metadata(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    rasters = payload.get("rasters")
    if not isinstance(rasters, list) or len(rasters) != 1 or not isinstance(rasters[0], dict):
        raise ValueError(f"unexpected raster metadata shape in {path}")
    return rasters[0]


def discover_single_raster(asset_path: Path) -> tuple[Path, list[Path]]:
    if not asset_path.exists():
        raise FileNotFoundError(f"CDL asset path does not exist: {asset_path}")
    candidates = discover_candidates(asset_path)
    selected = choose_candidate(candidates)
    if selected is None:
        candidate_text = ", ".join(str(candidate) for candidate in candidates)
        raise ValueError(
            "multiple CDL raster candidates found and no selector is supported in OS-004: "
            + candidate_text
        )
    return selected, candidates


def parse_envi_data_file(header_path: Path) -> Path | None:
    try:
        text = header_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None

    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip().lower() != "data file":
            continue
        cleaned = value.strip().strip("{}").strip().strip('"')
        if not cleaned:
            return None
        candidate = Path(cleaned)
        if not candidate.is_absolute():
            candidate = header_path.parent / candidate
        return candidate
    return None


def yanroy_sidecar_status(raster_path: Path) -> dict[str, Any]:
    candidates: list[Path] = []
    header_path = raster_path if raster_path.suffix.lower() == ".hdr" else Path(str(raster_path) + ".hdr")
    expected = parse_envi_data_file(header_path) if header_path.exists() else None
    if expected is not None:
        candidates.append(expected)
    candidates.extend(
        [
            header_path,
            raster_path,
            raster_path.with_suffix(".bil"),
            raster_path.with_suffix(".bsq"),
            raster_path.with_suffix(".bip"),
            raster_path.with_suffix(".img"),
            raster_path.with_suffix(".dat"),
            raster_path.with_suffix(".ige"),
        ]
    )

    seen: set[str] = set()
    unique = []
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)

    return {
        "header_path": str(header_path),
        "header_exists": header_path.exists(),
        "declared_data_file": str(expected) if expected is not None else None,
        "candidate_sidecars": [
            {"path": str(candidate), "exists": candidate.exists()} for candidate in unique
        ],
        "missing_declared_data_file": bool(expected is not None and not expected.exists()),
    }


def first_band_dtype(metadata: dict[str, Any]) -> str:
    bands = metadata.get("bands")
    if not isinstance(bands, list) or not bands:
        return ""
    first = bands[0]
    if not isinstance(first, dict):
        return ""
    dtype = first.get("dtype")
    return dtype if isinstance(dtype, str) else ""


def bounds_overlap(left: dict[str, Any], right: dict[str, Any]) -> bool | None:
    left_epsg = left.get("epsg")
    right_epsg = right.get("epsg")
    if left_epsg != right_epsg or not isinstance(left_epsg, int) or left_epsg == 0:
        return None
    left_bounds = left.get("bounds")
    right_bounds = right.get("bounds")
    if not isinstance(left_bounds, dict) or not isinstance(right_bounds, dict):
        return None

    try:
        return not (
            float(left_bounds["max_x"]) <= float(right_bounds["min_x"])
            or float(right_bounds["max_x"]) <= float(left_bounds["min_x"])
            or float(left_bounds["max_y"]) <= float(right_bounds["min_y"])
            or float(right_bounds["max_y"]) <= float(left_bounds["min_y"])
        )
    except (KeyError, TypeError, ValueError):
        return None


def copy_tree_contents(source_root: Path, destination_root: Path) -> None:
    for source in source_root.rglob("*"):
        if source.is_dir():
            continue
        relative = source.relative_to(source_root)
        destination = destination_root / relative
        ensure_parent_dir(destination)
        shutil.copy2(source, destination)


def artifact_descriptor(name: str, path: str) -> dict[str, str]:
    return {
        "name": name,
        "kind": "file",
        "format": "json",
        "path": path,
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    artifact_dir = artifact_dir_path(args.artifact_dir)
    output_root = Path(args.output_root).expanduser() if args.output_root else None

    cdl_asset_path = Path(args.cdl_asset_path)
    yanroy_raster = Path(args.yanroy_raster)
    if not yanroy_raster.exists():
        raise FileNotFoundError(f"Yan/Roy raster does not exist: {yanroy_raster}")

    cdl_raster, cdl_candidates = discover_single_raster(cdl_asset_path)

    cdl_response = run_geospatial(
        args.geospatial_executable,
        artifact_dir,
        "cdl_2010_raster_info.request.json",
        CDL_RESPONSE_ARTIFACT,
        raster_info_request("cdl_2010", cdl_raster, CDL_METADATA_ARTIFACT),
    )
    yanroy_response = run_geospatial(
        args.geospatial_executable,
        artifact_dir,
        "yanroy_h18v07_raster_info.request.json",
        YANROY_RESPONSE_ARTIFACT,
        raster_info_request("yanroy_h18v07", yanroy_raster, YANROY_METADATA_ARTIFACT),
    )

    cdl_metadata_path = artifact_dir / CDL_METADATA_ARTIFACT
    yanroy_metadata_path = artifact_dir / YANROY_METADATA_ARTIFACT
    cdl_metadata = load_raster_metadata(cdl_metadata_path)
    yanroy_metadata = load_raster_metadata(yanroy_metadata_path)
    overlap = bounds_overlap(cdl_metadata, yanroy_metadata)
    if overlap is False:
        raise ValueError("CDL and Yan/Roy raster bounds do not overlap in their shared CRS")

    yanroy_dtype = first_band_dtype(yanroy_metadata)
    yanroy_uint16_compatible = yanroy_dtype in UINT16_COMPATIBLE_DTYPES
    discovery_payload = {
        "cdl_asset_path": str(cdl_asset_path),
        "cdl_primary_raster_path": str(cdl_raster),
        "cdl_raster_candidates": [str(candidate) for candidate in cdl_candidates],
        "yanroy_raster_path": str(yanroy_raster),
        "yanroy_sidecars": yanroy_sidecar_status(yanroy_raster),
        "metadata_summary": {
            "cdl": cdl_metadata,
            "yanroy": yanroy_metadata,
        },
        "bounds_overlap_same_crs": overlap,
        "yanroy_field_id_uint16_metadata_check": {
            "band_dtype": yanroy_dtype,
            "appears_to_fit_uint16": yanroy_uint16_compatible,
            "range_check_required_in_os005": not yanroy_uint16_compatible,
        },
        "geospatial_responses": {
            "cdl": cdl_response,
            "yanroy": yanroy_response,
        },
    }
    write_json(artifact_dir / DISCOVERY_ARTIFACT, discovery_payload)

    if output_root is not None:
        copy_tree_contents(artifact_dir / "metadata", output_root / "metadata")

    output_payload = {
        "artifacts": [
            artifact_descriptor("cdl_2010_raster_info_json", CDL_METADATA_ARTIFACT),
            artifact_descriptor("yanroy_h18v07_raster_info_json", YANROY_METADATA_ARTIFACT),
            artifact_descriptor("input_discovery_json", DISCOVERY_ARTIFACT),
            artifact_descriptor("cdl_2010_raster_info_response_json", CDL_RESPONSE_ARTIFACT),
            artifact_descriptor("yanroy_h18v07_raster_info_response_json", YANROY_RESPONSE_ARTIFACT),
        ],
        "summary": {
            "cdl_primary_raster_path": str(cdl_raster),
            "yanroy_raster_path": str(yanroy_raster),
            "bounds_overlap_same_crs": overlap if overlap is not None else "unknown",
            "yanroy_field_id_appears_to_fit_uint16": yanroy_uint16_compatible,
        },
    }
    write_json(worker_output_path(), output_payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
