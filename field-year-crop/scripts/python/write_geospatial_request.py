#!/usr/bin/env python3
"""Write a geospatial request JSON document for later goet-geospatial calls."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from field_crop_common import ensure_parent_dir


SUPPORTED_OPERATIONS = {
    "raster_info",
    "align_to_grid",
    "raster_pair_value_counts",
}


def write_json(output_path: Path, payload: dict[str, object]) -> None:
    ensure_parent_dir(output_path)
    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation", required=True, choices=sorted(SUPPORTED_OPERATIONS))
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--input-raster")
    parser.add_argument("--source-raster")
    parser.add_argument("--like-raster")
    parser.add_argument("--resampling", default="nearest")
    parser.add_argument("--field-raster")
    parser.add_argument("--value-raster")
    parser.add_argument("--require-aligned-grid", action="store_true")
    parser.add_argument("--chunk-rows", type=int, default=1024)
    parser.add_argument("--field-dtype", default="uint16")
    parser.add_argument("--value-dtype", default="uint16")
    return parser


def build_request(args: argparse.Namespace) -> dict[str, object]:
    request: dict[str, object] = {"operation": args.operation}
    if args.operation == "raster_info":
        if not args.input_raster:
            raise ValueError("--input-raster is required for raster_info")
        request["input_raster"] = args.input_raster
        return request

    if args.operation == "align_to_grid":
        if not args.source_raster or not args.like_raster:
            raise ValueError("--source-raster and --like-raster are required for align_to_grid")
        request["source_raster"] = args.source_raster
        request["like_raster"] = args.like_raster
        request["resampling"] = args.resampling
        return request

    if args.operation == "raster_pair_value_counts":
        if not args.field_raster or not args.value_raster:
            raise ValueError("--field-raster and --value-raster are required for raster_pair_value_counts")
        request["field_raster"] = args.field_raster
        request["value_raster"] = args.value_raster
        request["require_aligned_grid"] = bool(args.require_aligned_grid)
        request["chunk_rows"] = args.chunk_rows
        request["field_dtype"] = args.field_dtype
        request["value_dtype"] = args.value_dtype
        return request

    raise ValueError(f"unsupported operation: {args.operation}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    request = build_request(args)
    write_json(Path(args.output_json), request)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
