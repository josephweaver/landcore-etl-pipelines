#!/usr/bin/env python3
"""Executable contract test for summarize_field_crop_counts.py."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "python" / "summarize_field_crop_counts.py"
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "counts"
INPUT_CSV = FIXTURE_DIR / "field_crop_counts_2010.csv"
EXPECTED_CSV = FIXTURE_DIR / "expected_field_crop_year_summary_2010.csv"


def run_summary(temp_dir: Path) -> tuple[Path, Path]:
    output_csv = temp_dir / "summary.csv"
    metadata_json = temp_dir / "summary.metadata.json"
    subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--counts-csv",
            str(INPUT_CSV),
            "--year",
            "2010",
            "--output-csv",
            str(output_csv),
            "--metadata-json",
            str(metadata_json),
        ],
        check=True,
    )
    return output_csv, metadata_json


def main() -> int:
    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        output_csv, metadata_json = run_summary(temp_dir)
        assert output_csv.read_text(encoding="utf-8") == EXPECTED_CSV.read_text(encoding="utf-8")

        metadata = json.loads(metadata_json.read_text(encoding="utf-8"))
        assert metadata["input_row_count"] == 4
        assert metadata["output_row_count"] == 4
        assert metadata["distinct_field_count"] == 3
        assert metadata["year"] == 2010
        assert len(metadata["input_sha256"]) == 64
        assert len(metadata["output_sha256"]) == 64

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
