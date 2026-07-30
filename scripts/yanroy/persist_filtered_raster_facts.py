#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import json
import shutil
from pathlib import Path
from typing import Any


def _resolve(path_text: str) -> Path:
    return Path(str(path_text or "")).expanduser().resolve()


def _vlog(verbose: bool, message: str) -> None:
    if verbose:
        print(f"[persist_filtered_raster_facts] {message}")


def _parse_patterns(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for token in str(raw or "").replace(";", ",").split(","):
        item = str(token or "").strip()
        if not item:
            continue
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _discover_latest_csv(search_glob_csv: str, *, verbose: bool) -> Path | None:
    candidates: list[Path] = []
    for pat in _parse_patterns(search_glob_csv):
        for raw in glob.glob(str(pat), recursive=True):
            p = Path(raw)
            if p.is_file():
                candidates.append(p.resolve())
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    pick = candidates[0]
    _vlog(verbose, f"discovered candidates={len(candidates)} selected={pick.as_posix()}")
    return pick


def persist_filtered_raster_facts(
    *,
    source_csv: str,
    search_glob_csv: str,
    output_csv: str,
    summary_json: str,
    allow_missing: bool,
    verbose: bool,
) -> dict[str, Any]:
    src = _resolve(source_csv) if str(source_csv or "").strip() else None
    if src is not None and not src.exists():
        _vlog(verbose, f"explicit source_csv not found: {src.as_posix()}")
        src = None
    if src is None and str(search_glob_csv or "").strip():
        src = _discover_latest_csv(search_glob_csv, verbose=verbose)

    out = _resolve(output_csv)
    summary_path = _resolve(summary_json)
    out.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    if src is None:
        if not allow_missing:
            raise FileNotFoundError("no filtered_raster_facts.csv found (source_csv/search_glob_csv)")
        summary = {
            "found": False,
            "source_csv": "",
            "output_csv": out.as_posix(),
            "copied": False,
        }
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
        return summary

    shutil.copy2(src, out)
    summary = {
        "found": True,
        "source_csv": src.as_posix(),
        "output_csv": out.as_posix(),
        "copied": True,
        "size_bytes": int(out.stat().st_size),
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if verbose:
        print(json.dumps(summary, indent=2))
    else:
        print(f"copied {src.as_posix()} -> {out.as_posix()}")
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(description="Persist filtered_raster_facts.csv into data/yanroy/meta")
    ap.add_argument("--source-csv", default="", help="Explicit source CSV path; if missing, fallback to --search-glob-csv")
    ap.add_argument(
        "--search-glob-csv",
        default="",
        help="Glob(s) to discover filtered_raster_facts.csv when --source-csv is not provided/found",
    )
    ap.add_argument("--output-csv", required=True, help="Destination CSV path")
    ap.add_argument("--summary-json", required=True, help="Summary JSON output path")
    ap.add_argument("--allow-missing", action="store_true", help="Do not fail if no source CSV is found")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    persist_filtered_raster_facts(
        source_csv=str(args.source_csv or ""),
        search_glob_csv=str(args.search_glob_csv or ""),
        output_csv=str(args.output_csv),
        summary_json=str(args.summary_json),
        allow_missing=bool(args.allow_missing),
        verbose=bool(args.verbose),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
