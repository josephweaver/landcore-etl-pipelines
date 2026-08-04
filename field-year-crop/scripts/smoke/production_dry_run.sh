#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
run_root="$root_dir/field-year-crop/.run/production-dry-run"
delivery_root="$run_root/delivery"
units_json="$run_root/work-units.json"

rm -rf "$run_root"
mkdir -p "$delivery_root/units/2010-h18v07" "$delivery_root/units/2011-h18v07"

write_unit() {
  local year="$1"
  local unit_dir="$delivery_root/units/${year}-h18v07"

  cat >"$unit_dir/field_crop_year_counts_${year}_h18v07.csv" <<EOF_COUNTS
field_id,crop_id,count
1,5,3
1,1,1
2,5,2
EOF_COUNTS

  cat >"$unit_dir/field_crop_year_summary_${year}_h18v07.csv" <<EOF_SUMMARY
field_id,year,crop_id,pixel_count,total_field_pixels,share,is_dominant,dominant_crop_id,dominant_crop_share
1,${year},1,1,4,0.250000,false,5,0.750000
1,${year},5,3,4,0.750000,true,5,0.750000
2,${year},5,2,2,1.000000,true,5,1.000000
EOF_SUMMARY

  cat >"$unit_dir/raster_info_${year}_h18v07.json" <<EOF_JSON
{"operation":"raster_info","year":${year},"tile":"h18v07"}
EOF_JSON

  cat >"$unit_dir/alignment_${year}_h18v07.metadata.json" <<EOF_JSON
{"operation":"align_to_grid","year":${year},"tile":"h18v07"}
EOF_JSON

  cat >"$unit_dir/pair_counts_${year}_h18v07.request.json" <<EOF_JSON
{"operation":"raster_pair_value_counts","options":{"require_aligned_grid":true}}
EOF_JSON

  cat >"$unit_dir/pair_counts_${year}_h18v07.metadata.json" <<EOF_JSON
{"operation":"raster_pair_value_counts","year":${year},"tile":"h18v07","require_aligned_grid":true}
EOF_JSON

  cat >"$unit_dir/validation_${year}_h18v07.json" <<EOF_JSON
{"status":"passed","year":${year},"tile":"h18v07","counts_row_count":3,"summary_row_count":3}
EOF_JSON
}

write_unit 2010
write_unit 2011

python3 - "$delivery_root" "$units_json" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
out = Path(sys.argv[2])
units = []
for year in (2010, 2011):
    unit_dir = root / "units" / f"{year}-h18v07"
    units.append({
        "year": year,
        "tile": "h18v07",
        "state": "IL",
        "counts_csv": str(unit_dir / f"field_crop_year_counts_{year}_h18v07.csv"),
        "summary_csv": str(unit_dir / f"field_crop_year_summary_{year}_h18v07.csv"),
        "raster_info_json": str(unit_dir / f"raster_info_{year}_h18v07.json"),
        "alignment_metadata_json": str(unit_dir / f"alignment_{year}_h18v07.metadata.json"),
        "pair_counts_request_json": str(unit_dir / f"pair_counts_{year}_h18v07.request.json"),
        "pair_counts_metadata_json": str(unit_dir / f"pair_counts_{year}_h18v07.metadata.json"),
        "validation_json": str(unit_dir / f"validation_{year}_h18v07.json"),
    })
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps({"work_units": units}, indent=2) + "\n", encoding="utf-8")
PY

python3 "$root_dir/field-year-crop/scripts/python/merge_field_crop_year_outputs.py" \
  --work-units-json "$units_json" \
  --output-dir "$delivery_root"

gorc_commit="unknown"
if git -C "$root_dir/../go-etl" rev-parse --short HEAD >/dev/null 2>&1; then
  gorc_commit="$(git -C "$root_dir/../go-etl" rev-parse --short HEAD)"
fi

python3 "$root_dir/field-year-crop/scripts/python/write_delivery_manifest.py" \
  --delivery-root "$delivery_root" \
  --work-units-json "$units_json" \
  --output-json "$delivery_root/delivery_manifest.json" \
  --workflow "field-year-crop/workflows/production-field-crop-year.workflow.json" \
  --project-json "$root_dir/land-core.project.json" \
  --gorc-commit "$gorc_commit" \
  --production-run-id "production-dry-run"

python3 "$root_dir/field-year-crop/scripts/python/write_gdrive_publish_plan.py" \
  --delivery-manifest "$delivery_root/delivery_manifest.json" \
  --output-json "$delivery_root/gdrive_publish_plan.json"

python3 "$root_dir/field-year-crop/scripts/python/validate_delivery_package.py" \
  --delivery-root "$delivery_root" \
  --delivery-manifest "$delivery_root/delivery_manifest.json" \
  --gdrive-publish-plan "$delivery_root/gdrive_publish_plan.json" \
  --work-units-json "$units_json" \
  --output-json "$delivery_root/delivery_validation.json"

echo "production dry run passed"
echo "delivery root: $delivery_root"
