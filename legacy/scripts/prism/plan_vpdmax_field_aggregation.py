#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Iterable


def _read_tiles(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8", errors="replace").strip()
    if not text:
        return []

    # Support either simple line-list format or CSV with tile_id/tile_coord columns.
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) == 1 and "," not in lines[0]:
        return [lines[0]]

    first = lines[0].lower()
    if "," not in first:
        return lines

    out: list[str] = []
    reader = csv.DictReader(lines)
    for row in reader:
        tile = (row.get("tile_id") or row.get("tile_coord") or "").strip()
        if tile:
            out.append(tile)
    if out:
        return sorted(set(out))

    # Fallback: first column values.
    for ln in lines:
        value = ln.split(",")[0].strip()
        if value and value.lower() not in {"tile_id", "tile_coord"}:
            out.append(value)
    return sorted(set(out))


def _iter_rasters(prism_dir: Path) -> Iterable[Path]:
    exts = {".tif", ".tiff", ".bil"}
    for p in prism_dir.rglob("*"):
        if p.is_file() and p.suffix.lower() in exts:
            yield p


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build planning manifests for PRISM vpdmax field aggregation (scaffold helper)."
    )
    ap.add_argument("--prism-dir", required=True, help="Directory containing staged PRISM vpdmax rasters.")
    ap.add_argument("--tiles-csv", required=True, help="tiles.of.interest.csv path (or line-list of tile IDs).")
    ap.add_argument("--output-dir", required=True, help="Output directory for planning files.")
    ap.add_argument("--months", default="7,8", help="Comma-separated month list to target (default: 7,8).")
    ap.add_argument("--year-start", type=int, default=2005)
    ap.add_argument("--year-end", type=int, default=2025)
    ap.add_argument(
        "--filename-regex",
        default=r"(?P<year>20\d{2})(?P<month>0[1-9]|1[0-2])",
        help="Regex with named groups 'year' and 'month' used to parse raster filenames.",
    )
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    prism_dir = Path(args.prism_dir).expanduser().resolve()
    tiles_csv = Path(args.tiles_csv).expanduser().resolve()
    out_dir = Path(args.output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not prism_dir.exists():
        raise FileNotFoundError(f"prism dir not found: {prism_dir}")
    if not tiles_csv.exists():
        raise FileNotFoundError(f"tiles csv not found: {tiles_csv}")

    tiles = _read_tiles(tiles_csv)
    if not tiles:
        raise RuntimeError(f"no tiles found in: {tiles_csv}")

    month_set = {int(x.strip()) for x in str(args.months).split(",") if x.strip()}
    year_start = int(args.year_start)
    year_end = int(args.year_end)
    pat = re.compile(str(args.filename_regex))

    available_rows: list[dict[str, object]] = []
    for path in _iter_rasters(prism_dir):
        m = pat.search(path.name)
        if not m:
            continue
        year = int(m.group("year"))
        month = int(m.group("month"))
        if year < year_start or year > year_end:
            continue
        if month not in month_set:
            continue
        available_rows.append(
            {
                "year": year,
                "month": month,
                "filename": path.name,
                "path": path.as_posix(),
            }
        )

    available_rows.sort(key=lambda r: (int(r["year"]), int(r["month"]), str(r["filename"])))

    ym_pairs = sorted({(int(r["year"]), int(r["month"])) for r in available_rows})
    planned_rows: list[dict[str, object]] = []
    for tile in tiles:
        for (year, month) in ym_pairs:
            planned_rows.append({"tile_id": tile, "year": year, "month": month})

    available_csv = out_dir / "available_vpdmax_files.csv"
    with available_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["year", "month", "filename", "path"])
        w.writeheader()
        w.writerows(available_rows)

    planned_csv = out_dir / "planned_tile_year_month_tasks.csv"
    with planned_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["tile_id", "year", "month"])
        w.writeheader()
        w.writerows(planned_rows)

    summary = {
        "prism_dir": prism_dir.as_posix(),
        "tiles_csv": tiles_csv.as_posix(),
        "tiles_count": len(tiles),
        "tiles": tiles,
        "months_filter": sorted(month_set),
        "year_start": year_start,
        "year_end": year_end,
        "available_files_count": len(available_rows),
        "available_year_month_count": len(ym_pairs),
        "planned_tasks_count": len(planned_rows),
        "available_csv": available_csv.as_posix(),
        "planned_csv": planned_csv.as_posix(),
        "note": "Planner scaffold only. Field-level raster aggregation step is not implemented in this script.",
    }
    summary_json = out_dir / "plan_summary.json"
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if args.verbose:
        print(json.dumps(summary, indent=2))
    else:
        print(
            f"planned tasks={summary['planned_tasks_count']} "
            f"(tiles={summary['tiles_count']} year_month_pairs={summary['available_year_month_count']})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

