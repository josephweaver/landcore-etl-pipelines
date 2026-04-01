#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path
from typing import Any


def main() -> int:
    ap = argparse.ArgumentParser(description="Combine per-tile YanRoy field-FIPS summaries into one summary JSON.")
    ap.add_argument("--summary-glob", required=True)
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--output-summary-json", required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    summary_glob = str(args.summary_glob or "").replace("\\", "/")
    summary_paths = sorted(Path(p).resolve() for p in glob.glob(summary_glob))
    if not summary_paths:
        raise RuntimeError(f"no summary files matched: {args.summary_glob}")

    tiles: list[str] = []
    output_row_count = 0
    ambiguous_field_count = 0
    dissolved_field_count = 0
    intersection_row_count = 0
    for path in summary_paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"summary is not a JSON object: {path}")
        tile = str(payload.get("tile") or "").strip()
        if tile:
            tiles.append(tile)
        output_row_count += int(payload.get("output_row_count") or 0)
        ambiguous_field_count += int(payload.get("ambiguous_field_count") or 0)
        dissolved_field_count += int(payload.get("dissolved_field_count") or 0)
        intersection_row_count += int(payload.get("intersection_row_count") or 0)

    out = {
        "output_csv": str(Path(args.output_csv).expanduser().resolve().as_posix()),
        "summary_glob": str(args.summary_glob),
        "summary_file_count": len(summary_paths),
        "tiles": tiles,
        "dissolved_field_count": dissolved_field_count,
        "intersection_row_count": intersection_row_count,
        "output_row_count": output_row_count,
        "ambiguous_field_count": ambiguous_field_count,
        "summary_files": [p.as_posix() for p in summary_paths],
    }
    output_summary_json = Path(str(args.output_summary_json)).expanduser().resolve()
    output_summary_json.parent.mkdir(parents=True, exist_ok=True)
    output_summary_json.write_text(json.dumps(out, indent=2), encoding="utf-8")
    if args.verbose:
        print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
