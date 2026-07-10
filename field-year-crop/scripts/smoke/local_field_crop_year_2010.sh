#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "$script_dir/../../.." && pwd)"
goetl_dir="$(cd "$root_dir/../go-etl" && pwd)"
canonical_runtime_root="/tmp/landcore-field-year-crop"
if [[ -n "${LANDCORE_TMP_ROOT:-}" ]]; then
  runtime_parent="$LANDCORE_TMP_ROOT"
elif [[ -d "/mnt/d" && -w "/mnt/d" ]]; then
  runtime_parent="/mnt/d/landcore-tmp"
else
  runtime_parent="/tmp"
fi
runtime_root="$runtime_parent/landcore-field-year-crop"
mkdir -p "$runtime_root"
if [[ "$runtime_root" != "$canonical_runtime_root" ]]; then
  rm -rf "$canonical_runtime_root"
  ln -s "$runtime_root" "$canonical_runtime_root"
fi
real_root="$canonical_runtime_root/local-field-crop-year-2010"
os004_root="$canonical_runtime_root/local-real-input-metadata-2010"
cdl_url="https://www.nass.usda.gov/Research_and_Science/Cropland/Release/datasets/2010_30m_cdls.zip"
cdl_zip="$real_root/source/2010_30m_cdls.zip"
cdl_extract="$real_root/cdl-2010"
metadata_dir="$real_root/metadata"
aligned_dir="$real_root/aligned"
counts_dir="$real_root/counts"
summary_dir="$real_root/summary"
validation_dir="$real_root/validation"
yanroy_stage_dir="$real_root/yanroy"
yanroy_raster="$yanroy_stage_dir/WELD_h18v07_2010_field_segments"
yanroy_header="$yanroy_raster.hdr"
repo_yanroy_dir="$root_dir/.data/h18v07"
repo_yanroy_header="$repo_yanroy_dir/WELD_h18v07_2010_field_segments.hdr"
release_archive="$root_dir/.data/ReleaseData.7z"
tmp_parent="$real_root/tmp"
mkdir -p "$tmp_parent"
tmp_root="$(mktemp -d "$tmp_parent/os005.XXXXXX")"
runner_root="$tmp_root/runner"
bin_dir="$tmp_root/bin"
go_tmp_dir="$tmp_root/go-tmp"
go_cache_dir="$tmp_root/go-cache"
worker_launcher="$bin_dir/worker-launcher.sh"
controller_log="$tmp_root/controller.log"
demo_log="$tmp_root/demo.log"
config_root="$tmp_root/configs"
controller_config="$config_root/controller/controller.json"
worker_runtime_root="$tmp_root/controller-runtime"
controller_pid=""

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

for command_name in go gdalinfo gdal_translate python3 curl; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "missing required command: $command_name" >&2
    exit 1
  fi
done

stage_yanroy_from_dir() {
  local source_dir="$1"

  rm -rf "$yanroy_stage_dir"
  mkdir -p "$yanroy_stage_dir"
  cp "$source_dir"/WELD_h18v07_2010_field_segments* "$yanroy_stage_dir"/
}

stage_yanroy_from_archive() {
  local extract_root="$real_root/yanroy-release-data"
  local seven_zip=""
  local source_header=""
  local source_dir=""

  for candidate in 7z 7za 7zr; do
    if command -v "$candidate" >/dev/null 2>&1; then
      seven_zip="$candidate"
      break
    fi
  done

  if [[ -z "$seven_zip" ]]; then
    echo "missing Yan/Roy directory: $repo_yanroy_dir" >&2
    echo "found $release_archive, but extracting it requires 7z, 7za, or 7zr" >&2
    return 1
  fi

  rm -rf "$extract_root"
  mkdir -p "$extract_root"
  "$seven_zip" x -y "-o$extract_root" "$release_archive" >/dev/null
  source_header="$(find "$extract_root" -type f -name "WELD_h18v07_2010_field_segments.hdr" | head -n 1)"
  if [[ -z "$source_header" ]]; then
    echo "ReleaseData.7z did not contain WELD_h18v07_2010_field_segments.hdr" >&2
    return 1
  fi

  source_dir="$(dirname "$source_header")"
  stage_yanroy_from_dir "$source_dir"
}

if [[ -f "$repo_yanroy_header" ]]; then
  stage_yanroy_from_dir "$repo_yanroy_dir"
elif [[ -f "$release_archive" ]]; then
  stage_yanroy_from_archive
else
  echo "missing Yan/Roy h18v07 input" >&2
  echo "expected $repo_yanroy_header or $release_archive" >&2
  exit 1
fi

if [[ ! -f "$yanroy_header" ]]; then
  echo "missing Yan/Roy raster header: $yanroy_header" >&2
  exit 1
fi
if [[ ! -f "$yanroy_raster" ]]; then
  echo "missing Yan/Roy raster data file: $yanroy_raster" >&2
  exit 1
fi
if ! gdalinfo "$yanroy_raster" >/dev/null 2>&1; then
  echo "GDAL cannot open Yan/Roy raster: $yanroy_raster" >&2
  gdalinfo "$yanroy_raster" >&2 || true
  exit 1
fi

mkdir -p "$real_root/source"
if [[ ! -f "$cdl_zip" ]]; then
  if [[ -f "$os004_root/source/2010_30m_cdls.zip" ]]; then
    echo "reusing CDL ZIP from $os004_root/source/2010_30m_cdls.zip" >&2
    cp "$os004_root/source/2010_30m_cdls.zip" "$cdl_zip"
  else
    echo "downloading CDL 2010 ZIP to $cdl_zip" >&2
    curl -L --fail --retry 3 --retry-delay 5 -o "$cdl_zip.tmp" "$cdl_url"
    mv "$cdl_zip.tmp" "$cdl_zip"
  fi
fi

rm -rf "$metadata_dir" "$aligned_dir" "$counts_dir" "$summary_dir" "$validation_dir"
mkdir -p "$metadata_dir" "$aligned_dir" "$counts_dir" "$summary_dir" "$validation_dir"
discovery_probe="$tmp_root/cdl-discovery.json"
if [[ -d "$cdl_extract" ]] && python3 "$root_dir/field-year-crop/scripts/python/discover_raster_asset.py" \
  --asset-path "$cdl_extract" \
  --output-json "$discovery_probe"; then
  echo "reusing extracted CDL under $cdl_extract" >&2
elif [[ -d "$os004_root/cdl-2010" ]] && python3 "$root_dir/field-year-crop/scripts/python/discover_raster_asset.py" \
  --asset-path "$os004_root/cdl-2010" \
  --output-json "$discovery_probe"; then
  echo "linking CDL extraction from $os004_root/cdl-2010" >&2
  rm -rf "$cdl_extract"
  ln -s "$os004_root/cdl-2010" "$cdl_extract"
else
  rm -rf "$cdl_extract"
  mkdir -p "$cdl_extract"
  if ! python3 - "$cdl_zip" "$cdl_extract" <<'PY'
import sys
import zipfile
from pathlib import Path

zip_path = Path(sys.argv[1])
extract_root = Path(sys.argv[2])
extract_root_resolved = extract_root.resolve()
with zipfile.ZipFile(zip_path) as archive:
    for member in archive.infolist():
        target = (extract_root / member.filename).resolve()
        if target != extract_root_resolved and extract_root_resolved not in target.parents:
            raise SystemExit(f"unsafe ZIP member path: {member.filename}")
    archive.extractall(extract_root)
PY
  then
    echo "failed to extract CDL archive: $cdl_zip" >&2
    exit 1
  fi

  if ! python3 "$root_dir/field-year-crop/scripts/python/discover_raster_asset.py" \
    --asset-path "$cdl_extract" \
    --output-json "$discovery_probe"; then
    echo "failed to discover a single CDL raster under $cdl_extract" >&2
    cat "$discovery_probe" >&2 || true
    exit 1
  fi
fi

mkdir -p "$runner_root" "$bin_dir" "$go_tmp_dir" "$go_cache_dir" "$config_root/controller"
ln -s "$goetl_dir" "$runner_root/go-etl"

cd "$runner_root/go-etl"
export GOTMPDIR="$go_tmp_dir"
export GOCACHE="$go_cache_dir"
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
    "name": "local-field-crop-year-2010",
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
        "data_dir": "$worker_runtime_root/data",
        "max_asset_bytes": 20000000000
      }
    }
  }
}
EOF

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

(
  cd "$root_dir"
  "$bin_dir/demo-client" submit \
    --controller-url "http://localhost:8080" \
    --project "land-core.project.json" \
    --workflow "field-year-crop/workflows/local-field-crop-year-2010.workflow.json"
) >"$demo_log" 2>&1

submission_id="$(sed -n 's/^Submission: //p' "$demo_log" | tail -n 1)"
if [[ -z "$submission_id" ]]; then
  echo "failed to read submission ID from demo-client output" >&2
  cat "$demo_log" >&2 || true
  exit 1
fi

wait_deadline=$((SECONDS + 1800))
last_status_log="$tmp_root/last-status.log"
while (( SECONDS < wait_deadline )); do
  set +e
  (
    cd "$root_dir"
    "$bin_dir/demo-client" status \
      --controller-url "http://localhost:8080" \
      "$submission_id"
  ) >"$last_status_log" 2>&1
  status_rc=$?
  set -e

  {
    echo
    echo "--- status poll $(date -u +%Y-%m-%dT%H:%M:%SZ) rc=$status_rc ---"
    cat "$last_status_log"
  } >>"$demo_log"

  if [[ "$status_rc" -eq 0 ]]; then
    if grep -q '^Status: completed$' "$last_status_log"; then
      break
    fi
    if grep -q '^Status: failed$' "$last_status_log"; then
      echo "workflow failed" >&2
      cat "$last_status_log" >&2
      exit 1
    fi
  fi

  sleep 5
done

if ! grep -q '^Status: completed$' "$last_status_log"; then
  echo "timed out waiting for workflow completion" >&2
  cat "$last_status_log" >&2 || true
  exit 1
fi

for path in \
  "$metadata_dir/input_discovery.json" \
  "$metadata_dir/raster_info.json" \
  "$aligned_dir/cdl_2010_on_h18v07_grid.tif" \
  "$aligned_dir/cdl_2010_on_h18v07_grid.metadata.json" \
  "$aligned_dir/yanroy_h18v07_uint32.tif" \
  "$counts_dir/field_crop_counts_2010.csv" \
  "$counts_dir/field_crop_counts_2010.metadata.json" \
  "$summary_dir/field_crop_year_summary_2010.csv" \
  "$summary_dir/field_crop_year_summary_2010.metadata.json" \
  "$validation_dir/field_crop_year_validation_2010.json"; do
  if [[ ! -f "$path" ]]; then
    echo "missing expected artifact: $path" >&2
    cat "$demo_log" >&2
    exit 1
  fi
done

python3 - "$validation_dir/field_crop_year_validation_2010.json" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if payload.get("status") != "passed":
    raise SystemExit("validation did not pass")
if payload.get("counts_row_count", 0) <= 0:
    raise SystemExit("counts row count must be positive")
if payload.get("summary_row_count") != payload.get("counts_row_count"):
    raise SystemExit("summary/count row count mismatch")
if payload.get("field_id_dtype") != "uint32":
    raise SystemExit("field_id_dtype must be uint32")
PY

if ! grep -q 'Status: completed' "$demo_log" || ! grep -q 'Failed: 0' "$demo_log"; then
  echo "workflow did not complete cleanly" >&2
  cat "$demo_log" >&2
  exit 1
fi

echo "local real field-crop-year workflow completed"
