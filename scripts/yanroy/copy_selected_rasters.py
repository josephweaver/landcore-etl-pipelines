#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import shutil
from pathlib import Path


def _copy_file(src: Path, dst: Path, *, overwrite: bool) -> bool:
    if not src.exists() or not src.is_file():
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() and not overwrite:
        return False
    shutil.copy2(src, dst)
    return True


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Copy rasters selected by geo_filter_rasters_by_polygon from src-root to dst-root."
    )
    ap.add_argument("--selected-csv", required=True, help="Path to selected_rasters.csv")
    ap.add_argument("--src-root", required=True, help="Source raster root directory")
    ap.add_argument("--dst-root", required=True, help="Destination filtered raster root directory")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing destination files")
    args = ap.parse_args()

    selected_csv = Path(args.selected_csv).expanduser().resolve()
    src_root = Path(args.src_root).expanduser().resolve()
    dst_root = Path(args.dst_root).expanduser().resolve()

    if not selected_csv.exists():
        raise FileNotFoundError(f"selected csv not found: {selected_csv}")
    if not src_root.exists():
        raise FileNotFoundError(f"src root not found: {src_root}")
    dst_root.mkdir(parents=True, exist_ok=True)

    copied = 0
    skipped = 0
    missing = 0
    seen: set[str] = set()

    with selected_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rel = str((row or {}).get("relative_path") or "").strip().replace("\\", "/")
            if not rel:
                continue
            candidates = [rel]
            if rel.lower().endswith(".hdr"):
                candidates.append(rel[:-4])
            for rel_path in candidates:
                key = rel_path.lower()
                if key in seen:
                    continue
                seen.add(key)
                src = src_root / rel_path
                dst = dst_root / rel_path
                if not src.exists():
                    missing += 1
                    continue
                if _copy_file(src, dst, overwrite=bool(args.overwrite)):
                    copied += 1
                else:
                    skipped += 1

    print(f"copied={copied} skipped={skipped} missing={missing}", end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
