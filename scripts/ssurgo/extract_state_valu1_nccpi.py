#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any


REQUIRED_COLUMNS = ["mukey", "nccpi3all", "nccpi3corn", "nccpi3soy"]


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    return str(value).strip()


def _resolve_ogr2ogr(path_text: str) -> str:
    raw = str(path_text or "").strip()
    if raw:
        return raw
    found = shutil.which("ogr2ogr")
    if found:
        return found
    raise RuntimeError("ogr2ogr not found; pass --ogr2ogr-bin or ensure it is on PATH")


def _run_ogr2ogr(*, ogr2ogr_bin: str, gdb_path: Path, tmp_csv: Path, layer_name: str, verbose: bool) -> None:
    sql = (
        "SELECT mukey, nccpi3all, nccpi3corn, nccpi3soy "
        f"FROM {layer_name}"
    )
    cmd = [
        ogr2ogr_bin,
        "-f",
        "CSV",
        tmp_csv.as_posix(),
        gdb_path.as_posix(),
        "-sql",
        sql,
        "-dialect",
        "OGRSQL",
        "-overwrite",
    ]
    if verbose:
        print("[extract_state_valu1_nccpi] running:", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            "ogr2ogr failed rc="
            f"{proc.returncode}: stdout={proc.stdout.strip()} stderr={proc.stderr.strip()}"
        )


def _rewrite_output(*, state_code: str, tmp_csv: Path, output_csv: Path) -> dict[str, Any]:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    distinct_mukeys: set[str] = set()

    with tmp_csv.open("r", encoding="utf-8-sig", newline="") as src:
        reader = csv.DictReader(src)
        fieldnames = list(reader.fieldnames or [])
        missing = [c for c in REQUIRED_COLUMNS if c not in fieldnames]
        if missing:
            raise ValueError(f"temporary csv missing required columns: {missing}")

        with output_csv.open("w", encoding="utf-8", newline="") as dst:
            writer = csv.DictWriter(dst, fieldnames=["state_code", *REQUIRED_COLUMNS])
            writer.writeheader()
            for row in reader:
                mukey = _normalize_text((row or {}).get("mukey"))
                if not mukey:
                    continue
                out = {"state_code": state_code}
                for col in REQUIRED_COLUMNS:
                    out[col] = _normalize_text((row or {}).get(col))
                writer.writerow(out)
                row_count += 1
                distinct_mukeys.add(mukey)

    return {
        "state_code": state_code,
        "output_csv": output_csv.as_posix(),
        "row_count": row_count,
        "distinct_mukey_count": len(distinct_mukeys),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Extract mukey and NCCPI columns from a state gSSURGO Valu1 table.")
    ap.add_argument("--state-code", required=True)
    ap.add_argument("--gdb-path", required=True)
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--ogr2ogr-bin", default="")
    ap.add_argument("--layer-name", default="Valu1")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    state_code = _normalize_text(args.state_code).upper()
    if len(state_code) != 2:
        raise ValueError("state_code must be a two-letter abbreviation")

    gdb_path = Path(str(args.gdb_path)).expanduser().resolve()
    output_csv = Path(str(args.output_csv)).expanduser().resolve()
    summary_json = Path(str(args.summary_json)).expanduser().resolve()
    if not gdb_path.exists():
        raise FileNotFoundError(f"gdb not found: {gdb_path}")

    ogr2ogr_bin = _resolve_ogr2ogr(str(args.ogr2ogr_bin))
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix=f"valu1_{state_code.lower()}_") as tmpdir:
        tmp_csv = Path(tmpdir) / f"{state_code}.valu1.tmp.csv"
        _run_ogr2ogr(
            ogr2ogr_bin=ogr2ogr_bin,
            gdb_path=gdb_path,
            tmp_csv=tmp_csv,
            layer_name=str(args.layer_name),
            verbose=bool(args.verbose),
        )
        summary = _rewrite_output(
            state_code=state_code,
            tmp_csv=tmp_csv,
            output_csv=output_csv,
        )

    summary.update(
        {
            "gdb_path": gdb_path.as_posix(),
            "ogr2ogr_bin": ogr2ogr_bin,
            "layer_name": str(args.layer_name),
        }
    )
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if args.verbose:
        print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
