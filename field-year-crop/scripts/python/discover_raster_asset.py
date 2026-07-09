#!/usr/bin/env python3
"""Discover a single raster asset from a file or directory."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from field_crop_common import ensure_parent_dir


ALLOWED_EXTENSIONS = {".tif", ".tiff", ".img", ".bil", ".hdr"}
PREFERRED_EXTENSIONS = {".tif", ".tiff", ".img", ".bil"}


def discover_candidates(asset_path: Path) -> list[Path]:
    if asset_path.is_file():
        return [asset_path.resolve()]

    candidates: list[Path] = []
    for child in asset_path.rglob("*"):
        if child.is_file() and child.suffix.lower() in ALLOWED_EXTENSIONS:
            candidates.append(child.resolve())
    candidates.sort(key=str)
    return candidates


def choose_candidate(candidates: list[Path]) -> Path | None:
    preferred = [candidate for candidate in candidates if candidate.suffix.lower() in PREFERRED_EXTENSIONS]
    if len(preferred) == 1:
        return preferred[0]
    if len(preferred) > 1:
        return None
    if len(candidates) == 1:
        return candidates[0]
    return None


def write_json(output_path: Path, payload: dict[str, object]) -> None:
    ensure_parent_dir(output_path)
    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--asset-path", required=True)
    parser.add_argument("--output-json", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    asset_path = Path(args.asset_path)
    output_path = Path(args.output_json)

    if not asset_path.exists():
        write_json(output_path, {"error": f"asset path does not exist: {asset_path}"})
        return 1

    candidates = discover_candidates(asset_path)
    candidate = choose_candidate(candidates)
    if candidate is None:
        write_json(
            output_path,
            {
                "error": "multiple raster candidates found; provide an explicit selector in a later slice",
                "candidates": [str(candidate) for candidate in candidates],
            },
        )
        return 1

    write_json(output_path, {"raster_path": str(candidate)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
