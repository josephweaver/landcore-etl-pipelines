from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from pathlib import Path
from typing import Iterable, List, Sequence


_IGNORED_SUFFIXES = {
    ".hdr",
    ".tfw",
    ".prj",
    ".aux",
    ".xml",
    ".ovr",
    ".cpg",
    ".dbf",
    ".shx",
    ".shp",
}


_TILE_ID_RE = re.compile(r"(?i)^h\d{2}v\d{2}$")
_TILE_ID_FIND_RE = re.compile(r"(?i)(h\d{2}v\d{2})")


def _normalize_tile_id(value: str) -> str:
    return str(value or "").strip().lower()


def _ordered_unique(items: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw in items:
        tile = _normalize_tile_id(raw)
        if not tile or tile in seen:
            continue
        seen.add(tile)
        out.append(tile)
    return out


def _read_tiles_of_interest(path: Path) -> List[str]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            raise ValueError("tiles.of.interest.csv has no header")
        if "tile_id" not in rdr.fieldnames:
            raise ValueError("tiles.of.interest.csv missing tile_id column")
        ordered: List[str] = []
        for row in rdr:
            ordered.append(str(row.get("tile_id") or ""))
    return _ordered_unique(ordered)


def _read_tiles_from_filtered_csv(path: Path) -> List[str]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            raise ValueError("filtered.csv has no header")
        names = [str(n or "").strip() for n in (rdr.fieldnames or [])]
        lowered = [n.lower() for n in names]
        tile_col = ""
        for candidate in ["tile_id", "tile", "tile_name"]:
            if candidate in lowered:
                tile_col = names[lowered.index(candidate)]
                break

        ordered: List[str] = []
        for row in rdr:
            value = ""
            if tile_col:
                value = str(row.get(tile_col) or "")
            if not value:
                for v in row.values():
                    m = _TILE_ID_FIND_RE.search(str(v or ""))
                    if m:
                        value = m.group(1)
                        break
            ordered.append(value)
    return _ordered_unique(ordered)


def _discover_tiles_from_fields_dir(fields_dir: Path) -> List[str]:
    if not fields_dir.exists() or not fields_dir.is_dir():
        return []
    tiles: List[str] = []
    for p in sorted(fields_dir.iterdir()):
        if not p.is_dir():
            continue
        tile = _normalize_tile_id(p.name)
        if _TILE_ID_RE.match(tile):
            tiles.append(tile)
    return _ordered_unique(tiles)


def _resolve_tiles(
    *,
    tiles_csv: Path | None,
    filtered_csv: Path | None,
    fields_dir: Path,
    verbose: bool = False,
) -> tuple[List[str], str]:
    if tiles_csv is not None:
        tiles = _read_tiles_of_interest(tiles_csv)
        if tiles:
            return tiles, "tiles_csv"
        if verbose:
            print(f"[build_google_maps_field_keys][WARN] tiles csv missing/empty: {tiles_csv.as_posix()}")

    if filtered_csv is not None:
        tiles = _read_tiles_from_filtered_csv(filtered_csv)
        if tiles:
            return tiles, "filtered_csv"
        if verbose:
            print(f"[build_google_maps_field_keys][WARN] filtered csv missing/empty: {filtered_csv.as_posix()}")

    tiles = _discover_tiles_from_fields_dir(fields_dir)
    if tiles:
        return tiles, "fields_dir"

    raise RuntimeError(
        "unable to determine tile ids from tiles csv, filtered csv, or fields_dir subfolders"
    )


def _candidate_rasters(tile_dir: Path) -> Sequence[Path]:
    if not tile_dir.exists() or not tile_dir.is_dir():
        return []
    candidates: List[Path] = []
    for p in sorted(tile_dir.rglob("*")):
        if not p.is_file():
            continue
        low_name = p.name.lower()
        if "field_segments" not in low_name:
            continue
        if low_name.endswith("_ancillary_data"):
            continue
        if p.suffix.lower() in _IGNORED_SUFFIXES:
            continue
        candidates.append(p)
    return candidates


def _extract_field_counts(raster_path: Path) -> Counter[int]:
    try:
        import numpy as np
        import rasterio
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("build_google_maps_field_keys requires rasterio and numpy") from exc

    counts: Counter[int] = Counter()
    with rasterio.open(raster_path) as ds:
        nodata = ds.nodata
        for _, window in ds.block_windows(1):
            arr = ds.read(1, window=window, masked=False)
            flat = arr.ravel()
            if nodata is not None:
                flat = flat[flat != nodata]
            flat = flat[flat > 0]
            if flat.size == 0:
                continue
            vals, freqs = np.unique(flat, return_counts=True)
            for val, freq in zip(vals.tolist(), freqs.tolist()):
                counts[int(val)] += int(freq)
    return counts


def _build_rows(tiles: Iterable[str], fields_dir: Path, verbose: bool = False) -> tuple[list[dict], list[dict]]:
    key_rows: List[dict] = []
    tile_rows: List[dict] = []

    for tile_id in tiles:
        tile_dir = (fields_dir / tile_id).resolve()
        candidates = _candidate_rasters(tile_dir)
        if not candidates:
            tile_rows.append(
                {
                    "tile_id": tile_id,
                    "status": "missing_raster",
                    "raster_path": "",
                    "field_count": 0,
                    "key_count": 0,
                }
            )
            if verbose:
                print(f"[build_google_maps_field_keys][WARN] no raster found for {tile_id} under {tile_dir.as_posix()}")
            continue

        raster_path = candidates[0]
        counts = _extract_field_counts(raster_path)
        keys = 0
        for field_id in sorted(counts.keys()):
            pk = f"{tile_id}_{field_id}"
            key_rows.append(
                {
                    "primary_key": pk,
                    "tile_id": tile_id,
                    "field_id": int(field_id),
                    "pixel_count": int(counts[field_id]),
                    "source_relative_path": raster_path.relative_to(fields_dir).as_posix(),
                }
            )
            keys += 1

        tile_rows.append(
            {
                "tile_id": tile_id,
                "status": "ok",
                "raster_path": raster_path.relative_to(fields_dir).as_posix(),
                "field_count": len(counts),
                "key_count": keys,
            }
        )
        if verbose:
            print(
                f"[build_google_maps_field_keys] tile={tile_id} raster={raster_path.name} "
                f"unique_fields={len(counts)}"
            )

    return key_rows, tile_rows


def build_google_maps_field_keys(
    tiles_csv: Path | None,
    filtered_csv: Path | None,
    fields_dir: Path,
    output_csv: Path,
    summary_json: Path,
    verbose: bool = False,
) -> int:
    if not fields_dir.exists() or not fields_dir.is_dir():
        raise FileNotFoundError(f"fields dir not found: {fields_dir}")
    tiles, tile_source = _resolve_tiles(
        tiles_csv=tiles_csv,
        filtered_csv=filtered_csv,
        fields_dir=fields_dir,
        verbose=verbose,
    )

    key_rows, tile_rows = _build_rows(tiles=tiles, fields_dir=fields_dir, verbose=verbose)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["primary_key", "tile_id", "field_id", "pixel_count", "source_relative_path"],
        )
        w.writeheader()
        w.writerows(key_rows)

    summary = {
        "tile_source": tile_source,
        "tiles_csv": tiles_csv.resolve().as_posix() if tiles_csv is not None else "",
        "filtered_csv": filtered_csv.resolve().as_posix() if filtered_csv is not None else "",
        "tiles_requested": len(tiles),
        "tiles_ok": sum(1 for r in tile_rows if r["status"] == "ok"),
        "tiles_missing_raster": sum(1 for r in tile_rows if r["status"] == "missing_raster"),
        "keys_total": len(key_rows),
        "tiles": tile_rows,
        "output_csv": output_csv.resolve().as_posix(),
    }
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if verbose:
        print(
            f"[build_google_maps_field_keys] requested_tiles={summary['tiles_requested']} "
            f"ok={summary['tiles_ok']} missing={summary['tiles_missing_raster']} keys={summary['keys_total']}"
        )
    return len(key_rows)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Build Google Maps key table from Yanroy field_id rasters. "
            "primary_key format: <tile_id>_<field_id>."
        )
    )
    ap.add_argument("--tiles-csv", default="", help="Optional path to tiles.of.interest.csv")
    ap.add_argument("--filtered-csv", default="", help="Optional path to filtered.csv containing tile ids")
    ap.add_argument("--fields-dir", required=True, help="Root directory of Yanroy tile folders")
    ap.add_argument("--output-csv", required=True, help="Output CSV for primary keys")
    ap.add_argument("--summary-json", required=True, help="Output summary JSON")
    ap.add_argument("--verbose", action="store_true")
    args, unknown = ap.parse_known_args(argv)
    if unknown:
        print(f"[build_google_maps_field_keys][WARN] ignoring unknown args: {' '.join(unknown)}")

    build_google_maps_field_keys(
        tiles_csv=(Path(args.tiles_csv).expanduser().resolve() if str(args.tiles_csv or "").strip() else None),
        filtered_csv=(Path(args.filtered_csv).expanduser().resolve() if str(args.filtered_csv or "").strip() else None),
        fields_dir=Path(args.fields_dir).expanduser().resolve(),
        output_csv=Path(args.output_csv).expanduser().resolve(),
        summary_json=Path(args.summary_json).expanduser().resolve(),
        verbose=args.verbose,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
