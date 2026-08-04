#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
goetl_dir="${GOETL_DIR:-$root_dir/../go-etl}"
run_root="$root_dir/field-year-crop/.run/hpcc-one-tile-2010"
workflow_template="$root_dir/field-year-crop/workflows/local-field-crop-year-2010.workflow.json"
workflow_rendered="$run_root/hpcc-field-crop-year-2010.workflow.json"
submission_log="$run_root/submission.log"
status_log="$run_root/last-status.log"

controller_url="${CONTROLLER_URL:-}"
controller_client_token_file="${CONTROLLER_CLIENT_TOKEN_FILE:-}"
hpcc_ssh_host="${HPCC_SSH_HOST:-dev-amd20.passwordless}"
ssh_bin="${SSH_BIN:-ssh}"
hpcc_scratch_root="${HPCC_SCRATCH_ROOT:-/mnt/scratch/weave151/etl}"
landcore_data_root="${LANDCORE_DATA_ROOT:-/mnt/scratch/weave151/data}"
yanroy_h18v07_path="${YANROY_H18V07_PATH:-$landcore_data_root/h18v07/WELD_h18v07_2010_field_segments}"
publish_root="${PUBLISH_ROOT:-$hpcc_scratch_root/publish}"
cdl_url="${CDL_2010_URL:-https://www.nass.usda.gov/Research_and_Science/Cropland/Release/datasets/2010_30m_cdls.zip}"
cdl_zip="$hpcc_scratch_root/source/2010_30m_cdls.zip"
cdl_root="$hpcc_scratch_root/source/cdl-2010"
product_root="$hpcc_scratch_root/publish/field-crop-year/h18v07/2010"
goet_cli="${GOET_CLI:-}"
wait_seconds="${HPCC_ONE_TILE_WAIT_SECONDS:-7200}"

usage() {
  cat >&2 <<'EOF'
Required environment:
  CONTROLLER_URL                 Public HTTPS GORC controller URL.
  CONTROLLER_CLIENT_TOKEN_FILE   Local client bearer token file for submit/status.

Usually also set before first run:
  GOET_CLI                       Path to goet/demo-client binary. If omitted, this script builds ../go-etl/cmd/demo-client.
  HPCC_SSH_HOST                  Default: dev-amd20.passwordless
  SSH_BIN                        Default: ssh
  HPCC_SCRATCH_ROOT              Default: /mnt/scratch/weave151/etl
  LANDCORE_DATA_ROOT             Default: /mnt/scratch/weave151/data
  YANROY_H18V07_PATH             Default: $LANDCORE_DATA_ROOT/h18v07/WELD_h18v07_2010_field_segments
  PUBLISH_ROOT                   Default: $HPCC_SCRATCH_ROOT/publish
EOF
}

require_value() {
  local name="$1"
  local value="$2"
  if [[ -z "$value" ]]; then
    echo "missing required environment variable: $name" >&2
    usage
    exit 2
  fi
}

render_json_string() {
  python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1"
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
mkdir -p "$HPCC_SCRATCH_ROOT/source" "$CDL_ROOT" "$PRODUCT_ROOT" "$PUBLISH_ROOT"
test -r "$YANROY_H18V07_PATH"
test -r "${YANROY_H18V07_PATH}.hdr"
if [ ! -s "$CDL_ZIP" ]; then
  tmp="${CDL_ZIP}.tmp.$$"
  rm -f "$tmp"
  curl -L --fail --retry 3 --retry-delay 5 -o "$tmp" "$CDL_2010_URL"
  mv "$tmp" "$CDL_ZIP"
fi
if ! find "$CDL_ROOT" -type f \( -name "*.tif" -o -name "*.tiff" -o -name "*.img" -o -name "*.bil" -o -name "*.hdr" \) | grep -q .; then
  rm -rf "$CDL_ROOT"
  mkdir -p "$CDL_ROOT"
  python3 - "$CDL_ZIP" "$CDL_ROOT" <<'"'"'PY'"'"'
import sys
import zipfile
from pathlib import Path
zip_path = Path(sys.argv[1])
out = Path(sys.argv[2])
with zipfile.ZipFile(zip_path) as archive:
    archive.extractall(out)
PY
fi
test -w "$PRODUCT_ROOT"
echo "prepared HPCC inputs"
echo "cdl_root=$CDL_ROOT"
echo "product_root=$PRODUCT_ROOT"
'
  "$ssh_bin" -o BatchMode=yes -o ConnectTimeout=20 "$hpcc_ssh_host" \
    "env HPCC_SCRATCH_ROOT=$(printf '%q' "$hpcc_scratch_root") CDL_ROOT=$(printf '%q' "$cdl_root") PRODUCT_ROOT=$(printf '%q' "$product_root") PUBLISH_ROOT=$(printf '%q' "$publish_root") YANROY_H18V07_PATH=$(printf '%q' "$yanroy_h18v07_path") CDL_ZIP=$(printf '%q' "$cdl_zip") CDL_2010_URL=$(printf '%q' "$cdl_url") bash -s" \
    <<<"$remote_script"
}

render_workflow() {
  mkdir -p "$run_root"
  if [[ ! -f "$workflow_template" ]]; then
    echo "missing workflow template: $workflow_template" >&2
    exit 1
  fi

  python3 - "$workflow_template" "$workflow_rendered" "$cdl_root" "$yanroy_h18v07_path" "$product_root" <<'PY'
import json
import sys
from pathlib import Path

template = Path(sys.argv[1])
output = Path(sys.argv[2])
cdl_root = sys.argv[3]
yanroy = sys.argv[4]
product_root = sys.argv[5]

payload = json.loads(template.read_text(encoding="utf-8"))
text = json.dumps(payload, indent=2)
text = text.replace("/tmp/landcore-field-year-crop/local-field-crop-year-2010/cdl-2010", cdl_root)
text = text.replace(
    "/tmp/landcore-field-year-crop/local-field-crop-year-2010/yanroy/WELD_h18v07_2010_field_segments",
    yanroy,
)
text = text.replace("/tmp/landcore-field-year-crop/local-field-crop-year-2010", product_root)
rendered = json.loads(text)
rendered["workflow"]["ID"] = "hpcc-field-crop-year-2010-h18v07"
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps(rendered, indent=2) + "\n", encoding="utf-8", newline="\n")
PY
  echo "rendered workflow: $workflow_rendered"
}

require_value "CONTROLLER_URL" "$controller_url"
require_value "CONTROLLER_CLIENT_TOKEN_FILE" "$controller_client_token_file"

if [[ "$controller_url" != https://* ]]; then
  echo "CONTROLLER_URL must use https for OS-007: $controller_url" >&2
  exit 2
fi
if [[ ! -s "$controller_client_token_file" ]]; then
  echo "controller client token file is missing or empty: $controller_client_token_file" >&2
  exit 2
fi

mkdir -p "$run_root"

if [[ "${SKIP_HPCC_PREFLIGHT:-0}" != "1" ]]; then
  bash "$root_dir/field-year-crop/scripts/smoke/hpcc_preflight.sh"
fi

remote_prepare_inputs
render_workflow
goet_bin="$(build_goet_cli)"

(
  cd "$root_dir"
  "$goet_bin" submit \
    --controller-url "$controller_url" \
    --controller-token-file "$controller_client_token_file" \
    --project "land-core.project.json" \
    --workflow "$workflow_rendered"
) >"$submission_log" 2>&1

submission_id="$(sed -n 's/^Submission: //p' "$submission_log" | tail -n 1)"
if [[ -z "$submission_id" ]]; then
  echo "failed to read submission ID from submit output" >&2
  cat "$submission_log" >&2 || true
  exit 1
fi

echo "submitted OS-007 one-tile workflow: $submission_id"
echo "submission log: $submission_log"

deadline=$((SECONDS + wait_seconds))
while (( SECONDS < deadline )); do
  set +e
  (
    cd "$root_dir"
    "$goet_bin" status \
      --controller-url "$controller_url" \
      --controller-token-file "$controller_client_token_file" \
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
      echo "OS-007 one-tile workflow completed"
      exit 0
    fi
    if grep -q "^Status: failed$" "$status_log"; then
      echo "OS-007 one-tile workflow failed" >&2
      cat "$status_log" >&2
      exit 1
    fi
  fi

  sleep 10
done

echo "timed out waiting for OS-007 one-tile workflow completion" >&2
cat "$status_log" >&2 || true
exit 1
