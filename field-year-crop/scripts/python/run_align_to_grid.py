#!/usr/bin/env python3
"""Prepare real inputs and align CDL 2010 to the Yan/Roy h18v07 grid."""

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


RASTER_INFO_ARTIFACT = "metadata/raster_info.json"
RASTER_INFO_RESPONSE_ARTIFACT = "raster_info.response.json"
ALIGN_REQUEST_ARTIFACT = "metadata/align_to_grid.request.json"
ALIGN_RESPONSE_ARTIFACT = "align_to_grid.response.json"
DISCOVERY_ARTIFACT = "metadata/input_discovery.json"
ALIGNED_CDL_ARTIFACT = "aligned/cdl_2010_on_h18v07_grid.tif"
ALIGNED_CDL_METADATA_ARTIFACT = "aligned/cdl_2010_on_h18v07_grid.metadata.json"
YANROY_UINT32_ARTIFACT = "aligned/yanroy_h18v07_uint32.tif"


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


def copy_tree_contents(source_root: Path, destination_root: Path) -> None:
    for source in source_root.rglob("*"):
        if source.is_dir():
            continue
        relative = source.relative_to(source_root)
        destination = destination_root / relative
        ensure_parent_dir(destination)
        shutil.copy2(source, destination)


def discover_single_raster(asset_path: Path) -> tuple[Path, list[Path]]:
    if not asset_path.exists():
        raise FileNotFoundError(f"CDL asset path does not exist: {asset_path}")
    candidates = discover_candidates(asset_path)
    selected = choose_candidate(candidates)
    if selected is None:
        candidate_text = ", ".join(str(candidate) for candidate in candidates)
        raise ValueError(
            "multiple CDL raster candidates found and no selector is supported in OS-005: "
            + candidate_text
        )
    return selected, candidates


def run_geospatial(
    executable: str,
    artifact_dir: Path,
    request_name: str,
    response_name: str,
    request_payload: dict[str, Any],
) -> dict[str, Any]:
    request_path = artifact_dir / request_name
    response_path = artifact_dir / response_name
    write_json(request_path, request_payload)
    subprocess.run(
        [executable, "--request", str(request_path), "--response", str(response_path)],
        check=True,
    )
    if not response_path.exists():
        raise FileNotFoundError(f"missing geospatial response: {response_path}")
    with response_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def raster_info_request(cdl_raster: Path, yanroy_raster: Path) -> dict[str, Any]:
    return {
        "api_version": "goet.geospatial/v1alpha1",
        "kind": "GeospatialOperationRequest",
        "operation": "raster_info",
        "inputs": {
            "cdl_2010": {"path": str(cdl_raster), "band": 1},
            "yanroy_h18v07": {"path": str(yanroy_raster), "band": 1},
        },
        "outputs": {"metadata_json": RASTER_INFO_ARTIFACT},
        "options": {},
    }


def align_request(cdl_raster: Path, target: dict[str, Any]) -> dict[str, Any]:
    return {
        "api_version": "goet.geospatial/v1alpha1",
        "kind": "GeospatialOperationRequest",
        "operation": "align_to_grid",
        "inputs": {
            "source_raster": {
                "path": str(cdl_raster),
                "band": 1,
                "nodata": 0,
            }
        },
        "outputs": {
            "raster_tif": ALIGNED_CDL_ARTIFACT,
            "metadata_json": ALIGNED_CDL_METADATA_ARTIFACT,
        },
        "options": {
            "target_crs": target["crs_wkt"],
            "target_transform": target["geo_transform"],
            "target_width": target["width"],
            "target_height": target["height"],
            "resampling": "nearest",
        },
    }


def load_raster_info(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    rasters = payload.get("rasters")
    if not isinstance(rasters, list):
        raise ValueError(f"missing raster metadata list in {path}")
    return {
        str(item.get("name")): item
        for item in rasters
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }


def load_gdal_json(raster_path: Path, compute_minmax: bool = False) -> dict[str, Any]:
    args = ["gdalinfo", "-json"]
    if compute_minmax:
        args.append("-mm")
    args.append(str(raster_path))
    completed = subprocess.run(args, check=True, text=True, capture_output=True)
    return json.loads(completed.stdout)


def find_wkt(value: Any) -> str:
    if isinstance(value, dict):
        for key, item in value.items():
            if key.lower() == "wkt" and isinstance(item, str) and item.strip():
                return item
            nested = find_wkt(item)
            if nested:
                return nested
    if isinstance(value, list):
        for item in value:
            nested = find_wkt(item)
            if nested:
                return nested
    return ""


def yanroy_target_grid(yanroy_raster: Path) -> dict[str, Any]:
    info = load_gdal_json(yanroy_raster)
    size = info.get("size")
    transform = info.get("geoTransform")
    wkt = find_wkt(info)
    if (
        not isinstance(size, list)
        or len(size) != 2
        or not isinstance(transform, list)
        or len(transform) != 6
        or not wkt
    ):
        raise ValueError("Yan/Roy raster is missing size, transform, or CRS WKT")
    return {
        "width": int(size[0]),
        "height": int(size[1]),
        "geo_transform": [float(value) for value in transform],
        "crs_wkt": wkt,
    }


def yanroy_minmax(yanroy_raster: Path) -> tuple[float, float, str]:
    info = load_gdal_json(yanroy_raster, compute_minmax=True)
    bands = info.get("bands")
    if not isinstance(bands, list) or not bands or not isinstance(bands[0], dict):
        raise ValueError("Yan/Roy raster has no band metadata")
    band = bands[0]
    dtype = str(band.get("type", ""))
    min_value = band.get("computedMin")
    max_value = band.get("computedMax")
    if not isinstance(min_value, (int, float)) or not isinstance(max_value, (int, float)):
        raise ValueError("Yan/Roy computed min/max are unavailable")
    return float(min_value), float(max_value), dtype


def create_uint32_yanroy(source: Path, destination: Path) -> None:
    ensure_parent_dir(destination)
    subprocess.run(
        [
            "gdal_translate",
            "-q",
            "-of",
            "GTiff",
            "-ot",
            "UInt32",
            "-a_nodata",
            "0",
            str(source),
            str(destination),
        ],
        check=True,
    )


def artifact_descriptor(name: str, kind: str, fmt: str, path: str) -> dict[str, str]:
    return {"name": name, "kind": kind, "format": fmt, "path": path}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    artifact_dir = artifact_dir_path(args.artifact_dir)
    output_root = Path(args.output_root).expanduser() if args.output_root else None
    cdl_raster, cdl_candidates = discover_single_raster(Path(args.cdl_asset_path))
    yanroy_raster = Path(args.yanroy_raster)
    if not yanroy_raster.exists():
        raise FileNotFoundError(f"Yan/Roy raster does not exist: {yanroy_raster}")

    min_value, max_value, source_dtype = yanroy_minmax(yanroy_raster)
    if min_value < 0:
        raise ValueError("Yan/Roy field IDs must be non-negative for uint32 pair counts")
    min_field_id = int(min_value)
    max_field_id = int(max_value)

    target = yanroy_target_grid(yanroy_raster)
    yanroy_uint32 = artifact_dir / YANROY_UINT32_ARTIFACT
    create_uint32_yanroy(yanroy_raster, yanroy_uint32)

    raster_info_response = run_geospatial(
        args.geospatial_executable,
        artifact_dir,
        "metadata/raster_info.request.json",
        RASTER_INFO_RESPONSE_ARTIFACT,
        raster_info_request(cdl_raster, yanroy_uint32),
    )
    raster_metadata = load_raster_info(artifact_dir / RASTER_INFO_ARTIFACT)

    align_response = run_geospatial(
        args.geospatial_executable,
        artifact_dir,
        ALIGN_REQUEST_ARTIFACT,
        ALIGN_RESPONSE_ARTIFACT,
        align_request(cdl_raster, target),
    )

    discovery_payload = {
        "cdl_asset_path": str(args.cdl_asset_path),
        "cdl_primary_raster_path": str(cdl_raster),
        "cdl_raster_candidates": [str(candidate) for candidate in cdl_candidates],
        "yanroy_raster_path": str(yanroy_raster),
        "yanroy_uint32_raster_path": str(yanroy_uint32),
        "yanroy_field_id_range": {
            "min": min_field_id,
            "max": max_field_id,
            "source_dtype": source_dtype,
            "field_dtype": "uint32",
            "uint32_working_copy": YANROY_UINT32_ARTIFACT,
        },
        "metadata_summary": raster_metadata,
        "target_grid_source": "yanroy_h18v07_explicit_wkt_transform",
        "geospatial_responses": {
            "raster_info": raster_info_response,
            "align_to_grid": align_response,
        },
    }
    write_json(artifact_dir / DISCOVERY_ARTIFACT, discovery_payload)

    if output_root is not None:
        copy_tree_contents(artifact_dir, output_root)

    output_payload = {
        "artifacts": [
            artifact_descriptor("input_discovery_json", "file", "json", DISCOVERY_ARTIFACT),
            artifact_descriptor("raster_info_json", "file", "json", RASTER_INFO_ARTIFACT),
            artifact_descriptor("aligned_cdl_tif", "file", "geotiff", ALIGNED_CDL_ARTIFACT),
            artifact_descriptor(
                "aligned_cdl_metadata_json",
                "file",
                "json",
                ALIGNED_CDL_METADATA_ARTIFACT,
            ),
            artifact_descriptor("yanroy_uint32_tif", "file", "geotiff", YANROY_UINT32_ARTIFACT),
        ],
        "summary": {
            "cdl_primary_raster_path": str(cdl_raster),
            "yanroy_raster_path": str(yanroy_raster),
            "yanroy_uint32_raster_path": str(output_root / YANROY_UINT32_ARTIFACT)
            if output_root is not None
            else str(yanroy_uint32),
            "aligned_cdl_raster_path": str(output_root / ALIGNED_CDL_ARTIFACT)
            if output_root is not None
            else str(artifact_dir / ALIGNED_CDL_ARTIFACT),
            "yanroy_field_id_max": max_field_id,
        },
    }
    write_json(worker_output_path(), output_payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
