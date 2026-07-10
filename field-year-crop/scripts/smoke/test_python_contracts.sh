#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

python3 "$root_dir/tests/test_summarize_field_crop_counts.py"

python3 "$root_dir/scripts/python/write_geospatial_request.py" \
  --operation raster_info \
  --input-raster "$tmp_dir/single.tif" \
  --output-json "$tmp_dir/raster-info-request.json"

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

if python3 - <<'PY' >/dev/null 2>&1
import numpy  # noqa: F401
from osgeo import gdal  # noqa: F401
PY
then
  python3 - <<'PY' "$tmp_dir"
import numpy as np
import sys
from pathlib import Path
from osgeo import gdal, osr

gdal.UseExceptions()
osr.UseExceptions()
tmp = Path(sys.argv[1])
driver = gdal.GetDriverByName("GTiff")
srs = osr.SpatialReference()
srs.ImportFromEPSG(5070)
projection = srs.ExportToWkt()
srs = None
transform = (0, 30, 0, 60, 0, -30)

def write_raster(path, array, gdal_type):
    ds = driver.Create(str(path), array.shape[1], array.shape[0], 1, gdal_type)
    ds.SetGeoTransform(transform)
    ds.SetProjection(projection)
    band = ds.GetRasterBand(1)
    band.SetNoDataValue(0)
    band.WriteArray(array)
    ds = None

write_raster(
    tmp / "fields_uint32.tif",
    np.array([[74000, 74000, 0], [2, 2, 74000]], dtype=np.uint32),
    gdal.GDT_UInt32,
)
write_raster(
    tmp / "crops_byte.tif",
    np.array([[5, 0, 7], [5, 5, 9]], dtype=np.uint8),
    gdal.GDT_Byte,
)
PY

  GOET_ARTIFACT_DIR="$tmp_dir/numpy-pair-artifacts" \
  GOET_OUTPUT_JSON="$tmp_dir/numpy-pair-output.json" \
  python3 "$root_dir/scripts/python/run_numpy_pair_counts.py" \
    --field-raster "$tmp_dir/fields_uint32.tif" \
    --value-raster "$tmp_dir/crops_byte.tif" \
    --year 2010 \
    --counts-csv "$tmp_dir/numpy-counts.csv" \
    --metadata-json "$tmp_dir/numpy-counts.metadata.json"

  python3 - <<'PY' "$tmp_dir/numpy-counts.csv" "$tmp_dir/numpy-counts.metadata.json"
import json
import sys
from pathlib import Path

counts = Path(sys.argv[1]).read_text(encoding="utf-8")
metadata = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
expected = "field_id,crop_id,count\n2,5,2\n74000,5,1\n74000,9,1\n"
assert counts == expected, counts
assert metadata["field_id_dtype"] == "uint32"
assert metadata["valid_pixels"] == 4
PY
fi

python3 -m py_compile \
  "$root_dir/scripts/python/field_crop_common.py" \
  "$root_dir/scripts/python/discover_raster_asset.py" \
  "$root_dir/scripts/python/run_align_to_grid.py" \
  "$root_dir/scripts/python/run_geospatial_pair_counts.py" \
  "$root_dir/scripts/python/run_numpy_pair_counts.py" \
  "$root_dir/scripts/python/run_raster_info.py" \
  "$root_dir/scripts/python/write_geospatial_request.py" \
  "$root_dir/scripts/python/summarize_field_crop_counts.py" \
  "$root_dir/scripts/python/validate_field_crop_year_product.py" \
  "$root_dir/tests/test_summarize_field_crop_counts.py"
