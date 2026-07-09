#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

python3 "$root_dir/tests/test_summarize_field_crop_counts.py"

python3 "$root_dir/scripts/python/write_geospatial_request.py" \
  --operation raster_pair_value_counts \
  --field-raster "$tmp_dir/field.tif" \
  --value-raster "$tmp_dir/value.tif" \
  --require-aligned-grid \
  --output-json "$tmp_dir/request.json"

touch "$tmp_dir/single.tif"

python3 "$root_dir/scripts/python/discover_raster_asset.py" \
  --asset-path "$tmp_dir/single.tif" \
  --output-json "$tmp_dir/discovered-single.json"

mkdir -p "$tmp_dir/raster-dir"
: > "$tmp_dir/raster-dir/a.hdr"
: > "$tmp_dir/raster-dir/b.tif"

python3 "$root_dir/scripts/python/discover_raster_asset.py" \
  --asset-path "$tmp_dir/raster-dir" \
  --output-json "$tmp_dir/discovered-dir.json"

python3 - <<'PY' "$tmp_dir/request.json" "$tmp_dir/discovered-single.json" "$tmp_dir/discovered-dir.json"
import json
import sys
from pathlib import Path

request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
single = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
directory = json.loads(Path(sys.argv[3]).read_text(encoding="utf-8"))

assert request["operation"] == "raster_pair_value_counts"
assert request["require_aligned_grid"] is True
assert single["raster_path"].endswith("single.tif")
assert directory["raster_path"].endswith("b.tif")
PY

python3 -m py_compile \
  "$root_dir/scripts/python/field_crop_common.py" \
  "$root_dir/scripts/python/discover_raster_asset.py" \
  "$root_dir/scripts/python/write_geospatial_request.py" \
  "$root_dir/scripts/python/summarize_field_crop_counts.py" \
  "$root_dir/tests/test_summarize_field_crop_counts.py"
