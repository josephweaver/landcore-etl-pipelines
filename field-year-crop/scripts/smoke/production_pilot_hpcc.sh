#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
goetl_dir="${GOETL_DIR:-$root_dir/../go-etl}"
run_id="${PRODUCTION_PILOT_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
run_root="$root_dir/field-year-crop/.run/production-pilot/$run_id"
workflow_template="$root_dir/field-year-crop/workflows/production-pilot-field-crop-year.workflow.json"
workflow_rendered="$run_root/production-pilot-field-crop-year.workflow.json"
submission_log="$run_root/submission.log"
status_log="$run_root/last-status.log"
run_report="$run_root/run-report.json"
secure_token_file="/tmp/goet-production-pilot-${run_id}-controller-token"

controller_url="${CONTROLLER_URL:-https://34-10-225-164.sslip.io}"
controller_client_token_file="${CONTROLLER_CLIENT_TOKEN_FILE:-}"
controller_ssh_host="${CONTROLLER_SSH_HOST:-instance-20260710-150616.us-central1-a.gorc-2026-07}"
hpcc_ssh_host="${HPCC_SSH_HOST:-dev-amd20.passwordless}"
ssh_bin="${SSH_BIN:-ssh}"
hpcc_scratch_root="${HPCC_SCRATCH_ROOT:-/mnt/scratch/weave151/etl}"
landcore_data_root="${LANDCORE_DATA_ROOT:-/mnt/scratch/weave151/data}"
product_root="${PRODUCT_ROOT:-$hpcc_scratch_root/publish/field-crop-year/production-pilot/$run_id}"
delivery_root="${DELIVERY_ROOT:-$hpcc_scratch_root/publish/field-crop-year-delivery/$run_id}"
years_csv="${PILOT_YEARS:-2010}"
tiles_csv="${PILOT_TILES:-h18v07,h23v08}"
publication_mode="${PUBLICATION_MODE:-plan_only}"
gdrive_remote="${GDRIVE_REMOTE:-gdrive}"
gdrive_delivery_base_path="${GDRIVE_DELIVERY_BASE_PATH:-Data/ETL/tile-field-year-crop}"
geospatial_executable="${GEOSPATIAL_EXECUTABLE:-/goetl/goet-geospatial}"
goet_cli="${GOET_CLI:-}"
wait_seconds="${PRODUCTION_PILOT_WAIT_SECONDS:-14400}"

if [[ -z "$controller_client_token_file" ]]; then
  candidate="$goetl_dir/.run/os009-secrets/controller-client-token"
  if [[ -s "$candidate" ]]; then
    controller_client_token_file="$candidate"
  fi
fi

usage() {
  cat >&2 <<'EOF'
Required environment:
  CONTROLLER_CLIENT_TOKEN_FILE   Local client bearer token file for submit/status.

Optional:
  CONTROLLER_URL                 Default: https://34-10-225-164.sslip.io
  PILOT_YEARS                    Default: 2010
  PILOT_TILES                    Default: h18v07,h23v08
  PUBLICATION_MODE               Default: plan_only
  GDRIVE_DELIVERY_BASE_PATH      Default: Data/ETL/tile-field-year-crop
EOF
}

require_file() {
  local label="$1"
  local path="$2"
  if [[ ! -s "$path" ]]; then
    echo "$label is missing or empty: $path" >&2
    usage
    exit 2
  fi
}

build_goet_cli() {
  if [[ -n "$goet_cli" ]]; then
    echo "$goet_cli"
    return
  fi
  if [[ ! -d "$goetl_dir" ]]; then
    echo "GOET_CLI is unset and go-etl checkout was not found at $goetl_dir" >&2
    exit 2
  fi
  local bin_dir="$run_root/bin"
  mkdir -p "$bin_dir"
  (
    cd "$goetl_dir"
    go build -o "$bin_dir/goet" ./cmd/demo-client
  )
  echo "$bin_dir/goet"
}

remote_prepare_inputs() {
  local remote_script
  remote_script='
set -euo pipefail
mkdir -p "$HPCC_SCRATCH_ROOT/source" "$PRODUCT_ROOT" "$DELIVERY_ROOT"
IFS=, read -r -a years <<< "$PILOT_YEARS"
IFS=, read -r -a tiles <<< "$PILOT_TILES"
for tile in "${tiles[@]}"; do
  raster="$LANDCORE_DATA_ROOT/$tile/WELD_${tile}_2010_field_segments"
  test -r "$raster"
  test -r "${raster}.hdr"
done
for year in "${years[@]}"; do
  zip="$HPCC_SCRATCH_ROOT/source/${year}_30m_cdls.zip"
  root="$HPCC_SCRATCH_ROOT/source/cdl-${year}"
  url="https://www.nass.usda.gov/Research_and_Science/Cropland/Release/datasets/${year}_30m_cdls.zip"
  if [ ! -s "$zip" ]; then
    tmp="${zip}.tmp.$$"
    rm -f "$tmp"
    curl -L --fail --retry 3 --retry-delay 5 -o "$tmp" "$url"
    mv "$tmp" "$zip"
  fi
  if ! find "$root" -type f \( -name "*.tif" -o -name "*.tiff" -o -name "*.img" -o -name "*.bil" -o -name "*.hdr" \) 2>/dev/null | grep -q .; then
    rm -rf "$root"
    mkdir -p "$root"
    python3 - "$zip" "$root" <<'"'"'PY'"'"'
import sys
import zipfile
from pathlib import Path
zip_path = Path(sys.argv[1])
out = Path(sys.argv[2])
with zipfile.ZipFile(zip_path) as archive:
    archive.extractall(out)
PY
  fi
done
echo "prepared production pilot inputs"
echo "product_root=$PRODUCT_ROOT"
echo "delivery_root=$DELIVERY_ROOT"
'
  "$ssh_bin" -o BatchMode=yes -o ConnectTimeout=20 "$hpcc_ssh_host" \
    "env HPCC_SCRATCH_ROOT=$(printf '%q' "$hpcc_scratch_root") LANDCORE_DATA_ROOT=$(printf '%q' "$landcore_data_root") PRODUCT_ROOT=$(printf '%q' "$product_root") DELIVERY_ROOT=$(printf '%q' "$delivery_root") PILOT_YEARS=$(printf '%q' "$years_csv") PILOT_TILES=$(printf '%q' "$tiles_csv") bash -s" \
    <<<"$remote_script"
}

render_workflow() {
  local gorc_commit="unknown"
  if git -C "$goetl_dir" rev-parse --short HEAD >/dev/null 2>&1; then
    gorc_commit="$(git -C "$goetl_dir" rev-parse --short HEAD)"
  fi
  python3 "$root_dir/field-year-crop/scripts/python/render_production_pilot_submission.py" \
    --template "$workflow_template" \
    --output "$workflow_rendered" \
    --project-json "$root_dir/land-core.project.json" \
    --years "$years_csv" \
    --tiles "$tiles_csv" \
    --hpcc-scratch-root "$hpcc_scratch_root" \
    --landcore-data-root "$landcore_data_root" \
    --product-root "$product_root" \
    --delivery-root "$delivery_root" \
    --publication-mode "$publication_mode" \
    --production-run-id "$run_id" \
    --gdrive-remote "$gdrive_remote" \
    --gdrive-delivery-base-path "$gdrive_delivery_base_path" \
    --gorc-commit "$gorc_commit" \
    --geospatial-executable "$geospatial_executable"
  echo "rendered workflow: $workflow_rendered"
}

require_file "controller client token file" "$controller_client_token_file"
mkdir -p "$run_root"
cp "$controller_client_token_file" "$secure_token_file"
chmod 600 "$secure_token_file"
trap 'rm -f "$secure_token_file"' EXIT

if [[ "${SKIP_PRODUCTION_PILOT_PREFLIGHT:-0}" != "1" ]]; then
  CONTROLLER_URL="$controller_url" \
  CONTROLLER_SSH_HOST="$controller_ssh_host" \
  HPCC_SSH_HOST="$hpcc_ssh_host" \
  SSH_BIN="$ssh_bin" \
  HPCC_SCRATCH_ROOT="$hpcc_scratch_root" \
  LANDCORE_DATA_ROOT="$landcore_data_root" \
  PILOT_YEARS="$years_csv" \
  PILOT_TILES="$tiles_csv" \
  GEOSPATIAL_EXECUTABLE="$geospatial_executable" \
  PUBLICATION_MODE="$publication_mode" \
  GDRIVE_REMOTE="$gdrive_remote" \
  GDRIVE_DELIVERY_BASE_PATH="$gdrive_delivery_base_path" \
    bash "$root_dir/field-year-crop/scripts/smoke/production_pilot_preflight.sh"
fi

remote_prepare_inputs
render_workflow
goet_bin="$(build_goet_cli)"

(
  cd "$root_dir"
  "$goet_bin" submit \
    --controller-url "$controller_url" \
    --controller-token-file "$secure_token_file" \
    --project "land-core.project.json" \
    --workflow "$workflow_rendered"
) >"$submission_log" 2>&1

submission_id="$(sed -n 's/^Submission: //p' "$submission_log" | tail -n 1)"
if [[ -z "$submission_id" ]]; then
  echo "failed to read submission ID from submit output" >&2
  cat "$submission_log" >&2 || true
  exit 1
fi

echo "submitted OS-009 production pilot workflow: $submission_id"
echo "submission log: $submission_log"

deadline=$((SECONDS + wait_seconds))
while (( SECONDS < deadline )); do
  set +e
  (
    cd "$root_dir"
    "$goet_bin" status \
      --controller-url "$controller_url" \
      --controller-token-file "$secure_token_file" \
      "$submission_id"
  ) >"$status_log" 2>&1
  status_rc=$?
  set -e

  {
    echo
    echo "--- status poll $(date -u +%Y-%m-%dT%H:%M:%SZ) rc=$status_rc ---"
    cat "$status_log"
  } >>"$submission_log"

  if [[ "$status_rc" -eq 0 ]]; then
    if grep -q "^Status: completed$" "$status_log"; then
      echo "OS-009 production pilot workflow completed"
      break
    fi
    if grep -q "^Status: failed$" "$status_log"; then
      echo "OS-009 production pilot workflow failed" >&2
      cat "$status_log" >&2
      exit 1
    fi
  fi

  sleep 20
done

if ! grep -q "^Status: completed$" "$status_log"; then
  echo "timed out waiting for OS-009 production pilot workflow completion" >&2
  cat "$status_log" >&2 || true
  exit 1
fi

worker_start_evidence=""
set +e
worker_start_evidence="$("$ssh_bin" -o BatchMode=yes -o ConnectTimeout=20 "$controller_ssh_host" "sudo journalctl -u gorc-controller --since '1 hour ago' --no-pager | grep -E 'worker_start_requested|worker_start_confirmed_by_registration|worker_capacity_evaluation' | tail -40" 2>/dev/null)"
set -e

python3 - "$run_report" "$submission_id" "$run_id" "$workflow_rendered" "$years_csv" "$tiles_csv" "$product_root" "$delivery_root" "$publication_mode" "$gdrive_remote" "$gdrive_delivery_base_path" "$status_log" "$worker_start_evidence" <<'PY'
import json
import sys
from pathlib import Path

report = {
    "submission_id": sys.argv[2],
    "production_run_id": sys.argv[3],
    "workflow_rendered": sys.argv[4],
    "years": [int(item) for item in sys.argv[5].split(",") if item],
    "tiles": [item for item in sys.argv[6].split(",") if item],
    "product_root": sys.argv[7],
    "delivery_root": sys.argv[8],
    "publication_mode": sys.argv[9],
    "gdrive_remote": sys.argv[10],
    "gdrive_delivery_base_path": sys.argv[11],
    "gdrive_delivery_zip_path": f"{sys.argv[11].strip('/')}/{sys.argv[3]}/tile-field-year-crop-delivery.zip",
    "status_log": sys.argv[12],
    "worker_start_evidence": sys.argv[13].splitlines() if sys.argv[13] else [],
}
report["expected_year_tile_pairs"] = len(report["years"]) * len(report["tiles"])
Path(sys.argv[1]).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
PY

echo "run report: $run_report"
echo "delivery root: $delivery_root"
