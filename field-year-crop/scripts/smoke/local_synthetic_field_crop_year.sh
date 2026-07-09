#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "$script_dir/../../.." && pwd)"
goetl_dir="$(cd "$root_dir/../go-etl" && pwd)"
synthetic_root="/tmp/landcore-field-year-crop/local-synthetic"
tmp_root="$(mktemp -d)"
runner_root="$tmp_root/runner"
bin_dir="$tmp_root/bin"
worker_launcher="$bin_dir/worker-launcher.sh"
controller_log="$tmp_root/controller.log"
worker_log="$tmp_root/worker.log"
demo_log="$tmp_root/demo.log"
controller_pid=""
config_root="$tmp_root/configs"
controller_config="$config_root/controller/controller.json"
worker_runtime_root="$tmp_root/controller-runtime"

cleanup() {
  local status=$?
  trap - EXIT

  if [[ -n "$controller_pid" ]] && kill -0 "$controller_pid" 2>/dev/null; then
    kill "$controller_pid" 2>/dev/null || true
    wait "$controller_pid" 2>/dev/null || true
  fi

  rm -rf "$runner_root"
  exit "$status"
}
trap cleanup EXIT

for command_name in go gdal_translate gdalinfo python3; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "missing required command: $command_name" >&2
    exit 1
  fi
done

rm -rf "$synthetic_root"
mkdir -p "$synthetic_root" "$runner_root" "$bin_dir" "$config_root/controller" \
  "$runner_root/go-etl-demo-project/field-year-crop/workflows" \
  "$runner_root/go-etl-demo-project/field-year-crop/scripts/python" \
  "$runner_root/go-etl-demo-project/workflows" \
  "$runner_root/go-etl-demo-project/scripts/python" \
  "$runner_root/go-etl-demo-project/submissions"
ln -s "$goetl_dir" "$runner_root/go-etl"
cp "$root_dir/land-core.project.json" "$runner_root/go-etl-demo-project/project.json"
cp "$root_dir/land-core.project.json" "$runner_root/go-etl-demo-project/land-core.project.json"
cp "$root_dir/field-year-crop/workflows/local-synthetic-field-crop-year.workflow.json" \
  "$runner_root/go-etl-demo-project/workflows/demo-workflow.json"
cp "$root_dir/field-year-crop/workflows/local-synthetic-field-crop-year.workflow.json" \
  "$runner_root/go-etl-demo-project/field-year-crop/workflows/local-synthetic-field-crop-year.workflow.json"
cp "$root_dir/field-year-crop/scripts/python/run_geospatial_pair_counts.py" \
  "$runner_root/go-etl-demo-project/scripts/python/run_geospatial_pair_counts.py"
cp "$root_dir/field-year-crop/scripts/python/run_geospatial_pair_counts.py" \
  "$runner_root/go-etl-demo-project/field-year-crop/scripts/python/run_geospatial_pair_counts.py"
cp "$root_dir/field-year-crop/scripts/python/summarize_field_crop_counts.py" \
  "$runner_root/go-etl-demo-project/scripts/python/summarize_field_crop_counts.py"
cp "$root_dir/field-year-crop/scripts/python/summarize_field_crop_counts.py" \
  "$runner_root/go-etl-demo-project/field-year-crop/scripts/python/summarize_field_crop_counts.py"
cp "$root_dir/field-year-crop/scripts/python/field_crop_common.py" \
  "$runner_root/go-etl-demo-project/scripts/python/field_crop_common.py"
cp "$root_dir/field-year-crop/scripts/python/field_crop_common.py" \
  "$runner_root/go-etl-demo-project/field-year-crop/scripts/python/field_crop_common.py"
cat >"$runner_root/go-etl-demo-project/submissions/demo-workflow-run.json" <<EOF
{
  "project": {
    "repository": "local:demo",
    "ref": "working-tree",
    "path": "project.json"
  },
  "workflow": {
    "repository": "local:demo",
    "ref": "working-tree",
    "path": "workflows/demo-workflow.json"
  },
  "variables": []
}
EOF

cd "$runner_root/go-etl"
go build -tags gdal -o "$bin_dir/goet-geospatial" ./cmd/goet-geospatial
go build -o "$bin_dir/controller" ./cmd/controller
go build -o "$bin_dir/worker" ./cmd/worker
go build -o "$bin_dir/demo-client" ./cmd/demo-client
cat >"$worker_launcher" <<EOF
#!/usr/bin/env bash
exec "$bin_dir/worker" "\$@"
EOF
chmod +x "$worker_launcher"
export PATH="$bin_dir:$PATH"
cp "$runner_root/go-etl/cmd/controller/defaults.json" "$config_root/controller/defaults.json"
cat >"$controller_config" <<EOF
{
  "api_version": "goet/v1alpha1",
  "kind": "Controller",
  "variables": [
    {
      "name": {
        "namespace": "controller_config",
        "key": "controller_url"
      },
      "type": "string",
      "expression": "http://localhost:8080"
    },
    {
      "name": {
        "namespace": "controller_config",
        "key": "main_database_driver"
      },
      "type": "string",
      "expression": "sqlite"
    },
    {
      "name": {
        "namespace": "controller_config",
        "key": "main_database_connection_string"
      },
      "type": "string",
      "expression": "$tmp_root/controller/workflow-execution.sqlite"
    }
  ],
  "execution_environment": {
    "name": "local-synthetic",
    "transports": [
      {
        "name": "local",
        "type": "local"
      }
    ],
    "dialect": {
      "type": "bash"
    },
    "scheduler": {
      "type": "direct_process"
    },
    "runtime": {
      "type": "worker",
      "settings": {
        "root": "$worker_runtime_root",
        "controller_url": "http://localhost:8080",
        "local_worker_artifact": "$worker_launcher",
        "data_dir": "$worker_runtime_root/data"
      }
    }
  }
}
EOF

write_grid() {
  local path="$1"
  local row1="$2"
  local row2="$3"
  local row3="$4"

  cat >"$path" <<EOF
ncols 3
nrows 3
xllcorner 0
yllcorner 0
cellsize 30
NODATA_value 0
$row1
$row2
$row3
EOF
}

translate_grid() {
  local asc_path="$1"
  local tif_path="$2"
  gdal_translate -q -of GTiff -ot UInt16 -a_nodata 0 -a_srs EPSG:5070 -a_ullr 0 90 90 0 "$asc_path" "$tif_path"
  gdalinfo "$tif_path" >/dev/null
}

field_asc="$synthetic_root/field.asc"
field_tif="$synthetic_root/field.tif"
cdl_asc="$synthetic_root/cdl.asc"
cdl_tif="$synthetic_root/cdl.tif"

write_grid "$field_asc" "1 1 2" "1 2 2" "3 3 3"
write_grid "$cdl_asc" "5 5 1" "5 1 1" "2 2 4"
translate_grid "$field_asc" "$field_tif"
translate_grid "$cdl_asc" "$cdl_tif"

submission_file="$root_dir/field-year-crop/submissions/local-synthetic-field-crop-year.submission.json"

cd "$runner_root/go-etl"
"$bin_dir/controller" --config "$controller_config" >"$controller_log" 2>&1 &
controller_pid=$!

wait_for_http() {
  local url="$1"
  local label="$2"

  for _ in $(seq 1 60); do
    if python3 - "$url" <<'PY' >/dev/null 2>&1
import sys
from urllib.request import urlopen

with urlopen(sys.argv[1], timeout=2) as response:
    response.read()
PY
    then
      return 0
    fi
    sleep 1
  done

  echo "timed out waiting for $label" >&2
  return 1
}

wait_for_http "http://localhost:8080/status" "controller"

timeout 600s "$bin_dir/demo-client" >"$demo_log" 2>&1

counts_csv="$synthetic_root/field_crop_year_counts.csv"
counts_metadata="$synthetic_root/field_crop_year_counts.metadata.json"
response_json="$synthetic_root/raster_pair_value_counts.response.json"
summary_csv="$synthetic_root/field_crop_year_summary.csv"
summary_metadata="$synthetic_root/field_crop_year_summary.metadata.json"

for path in "$counts_csv" "$counts_metadata" "$response_json" "$summary_csv" "$summary_metadata"; do
  if [[ ! -f "$path" ]]; then
    echo "missing expected artifact: $path" >&2
    exit 1
  fi
done

python3 - "$counts_csv" "$summary_csv" "$counts_metadata" "$summary_metadata" "$response_json" <<'PY'
import json
import sys
from pathlib import Path

counts_csv = Path(sys.argv[1])
summary_csv = Path(sys.argv[2])
counts_metadata = Path(sys.argv[3])
summary_metadata = Path(sys.argv[4])
response_json = Path(sys.argv[5])

expected_counts = """field_id,crop_id,count
1,5,3
2,1,3
3,2,2
3,4,1
"""
expected_summary = """field_id,year,crop_id,pixel_count,total_field_pixels,share,is_dominant,dominant_crop_id,dominant_crop_share
1,2010,5,3,3,1.000000,true,5,1.000000
2,2010,1,3,3,1.000000,true,1,1.000000
3,2010,2,2,3,0.666667,true,2,0.666667
3,2010,4,1,3,0.333333,false,2,0.666667
"""

actual_counts = counts_csv.read_text(encoding="utf-8")
actual_summary = summary_csv.read_text(encoding="utf-8")
if actual_counts != expected_counts:
    raise SystemExit(f"counts CSV mismatch:\n{actual_counts!r}")
if actual_summary != expected_summary:
    raise SystemExit(f"summary CSV mismatch:\n{actual_summary!r}")

counts_meta = json.loads(counts_metadata.read_text(encoding="utf-8"))
summary_meta = json.loads(summary_metadata.read_text(encoding="utf-8"))
response = json.loads(response_json.read_text(encoding="utf-8"))

if summary_meta["year"] != 2010:
    raise SystemExit("summary year metadata mismatch")
if len(summary_meta["input_sha256"]) != 64 or len(summary_meta["output_sha256"]) != 64:
    raise SystemExit("summary metadata sha256 length mismatch")
if counts_meta["valid_pixels"] != 9 or counts_meta["distinct_fields"] != 3 or counts_meta["distinct_pairs"] != 4:
    raise SystemExit("counts metadata mismatch")
if response.get("operation") != "raster_pair_value_counts":
    raise SystemExit("unexpected geospatial response operation")
if not response.get("artifacts"):
    raise SystemExit("missing geospatial response artifacts")
PY

if ! grep -q 'failed=0' "$demo_log"; then
  echo "workflow did not complete cleanly" >&2
  cat "$demo_log" >&2
  exit 1
fi

echo "local synthetic field-crop-year workflow completed"
