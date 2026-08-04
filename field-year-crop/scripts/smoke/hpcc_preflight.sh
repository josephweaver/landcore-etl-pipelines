#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

controller_url="${CONTROLLER_URL:-}"
controller_client_token_file="${CONTROLLER_CLIENT_TOKEN_FILE:-}"
hpcc_ssh_host="${HPCC_SSH_HOST:-dev-amd20.passwordless}"
ssh_bin="${SSH_BIN:-ssh}"
hpcc_scratch_root="${HPCC_SCRATCH_ROOT:-/mnt/scratch/weave151/etl}"
landcore_data_root="${LANDCORE_DATA_ROOT:-/mnt/scratch/weave151/data}"
yanroy_h18v07_path="${YANROY_H18V07_PATH:-$landcore_data_root/h18v07/WELD_h18v07_2010_field_segments}"
yanroy_release_archive="${YANROY_RELEASE_ARCHIVE:-$landcore_data_root/ReleaseData.7z}"
publish_root="${PUBLISH_ROOT:-$hpcc_scratch_root/publish}"
gorc_gdal_image="${GORC_GDAL_IMAGE:-}"
container_worker_executable="${CONTAINER_GOET_WORKER_EXECUTABLE:-/goetl/goetl-worker}"
worker_controller_token_file_hpcc="${WORKER_CONTROLLER_TOKEN_FILE_HPCC:-}"
cdl_url="${CDL_2010_URL:-https://www.nass.usda.gov/Research_and_Science/Cropland/Release/datasets/2010_30m_cdls.zip}"

slurm_partition="${SLURM_PARTITION:-}"
slurm_account="${SLURM_ACCOUNT:-}"
slurm_time="${SLURM_TIME:-00:05:00}"
slurm_mem="${SLURM_MEM:-1G}"
slurm_cpus_per_task="${SLURM_CPUS_PER_TASK:-1}"

usage() {
  cat >&2 <<'EOF'
Required environment:
  CONTROLLER_URL                         Public HTTPS GORC controller URL.
  CONTROLLER_CLIENT_TOKEN_FILE           Local client bearer token file for /status.
  GORC_GDAL_IMAGE                        HPCC Singularity/Apptainer image path.
  WORKER_CONTROLLER_TOKEN_FILE_HPCC      HPCC path to worker bearer token file.

Optional environment:
  HPCC_SSH_HOST                          Default: dev-amd20.passwordless
  SSH_BIN                                Default: ssh
  HPCC_SCRATCH_ROOT                      Default: /mnt/scratch/weave151/etl
  LANDCORE_DATA_ROOT                     Default: /mnt/scratch/weave151/data
  YANROY_H18V07_PATH                     Default: $LANDCORE_DATA_ROOT/h18v07/WELD_h18v07_2010_field_segments
  YANROY_RELEASE_ARCHIVE                 Default: $LANDCORE_DATA_ROOT/ReleaseData.7z
  PUBLISH_ROOT                           Default: $HPCC_SCRATCH_ROOT/publish
  CONTAINER_GOET_WORKER_EXECUTABLE       Default: /goetl/goetl-worker
  SLURM_PARTITION, SLURM_ACCOUNT, SLURM_TIME, SLURM_MEM, SLURM_CPUS_PER_TASK
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

controller_endpoint() {
  local path="$1"
  local base="${controller_url%/}"
  echo "$base$path"
}

curl_with_bearer_file() {
  local url="$1"
  local token_file="$2"
  local token
  local curl_config

  if [[ ! -s "$token_file" ]]; then
    echo "controller client token file is missing or empty: $token_file" >&2
    return 1
  fi

  token="$(tr -d '\r\n' <"$token_file")"
  if [[ -z "$token" ]]; then
    echo "controller client token file is empty after trimming newlines: $token_file" >&2
    return 1
  fi

  curl_config="$(mktemp)"
  chmod 600 "$curl_config"
  trap 'rm -f "$curl_config"' RETURN
  {
    printf 'fail\n'
    printf 'silent\n'
    printf 'show-error\n'
    printf 'max-time = 20\n'
    printf 'header = "Authorization: Bearer %s"\n' "$token"
  } >"$curl_config"
  curl --config "$curl_config" "$url" >/dev/null
}

remote_quote() {
  printf '%q' "$1"
}

run_remote() {
  local script="$1"
  shift
  "$ssh_bin" -o BatchMode=yes -o ConnectTimeout=20 "$hpcc_ssh_host" "$@" 'bash -s' <<<"$script"
}

require_value "CONTROLLER_URL" "$controller_url"
require_value "CONTROLLER_CLIENT_TOKEN_FILE" "$controller_client_token_file"
require_value "GORC_GDAL_IMAGE" "$gorc_gdal_image"
require_value "WORKER_CONTROLLER_TOKEN_FILE_HPCC" "$worker_controller_token_file_hpcc"

if [[ "$controller_url" != https://* ]]; then
  echo "CONTROLLER_URL must use https for OS-007: $controller_url" >&2
  exit 2
fi

echo "OS-007 HPCC preflight"
echo "repo root: $root_dir"
echo "controller URL: $controller_url"
echo "HPCC SSH host: $hpcc_ssh_host"
echo "SSH binary: $ssh_bin"
echo "HPCC scratch root: $hpcc_scratch_root"
echo "LandCore data root: $landcore_data_root"
echo "Yan/Roy h18v07: $yanroy_h18v07_path"
echo "publish root: $publish_root"
echo "worker image: $gorc_gdal_image"
echo "worker token file on HPCC: $worker_controller_token_file_hpcc"
echo

echo "checking controller HTTPS from local side"
curl --fail --silent --show-error --max-time 20 "$(controller_endpoint "/healthz")" >/dev/null
curl_with_bearer_file "$(controller_endpoint "/status")" "$controller_client_token_file"
echo "local controller HTTPS checks passed"
echo

remote_env=(
  "CONTROLLER_URL=$controller_url"
  "HPCC_SCRATCH_ROOT=$hpcc_scratch_root"
  "LANDCORE_DATA_ROOT=$landcore_data_root"
  "YANROY_H18V07_PATH=$yanroy_h18v07_path"
  "YANROY_RELEASE_ARCHIVE=$yanroy_release_archive"
  "PUBLISH_ROOT=$publish_root"
  "GORC_GDAL_IMAGE=$gorc_gdal_image"
  "CONTAINER_GOET_WORKER_EXECUTABLE=$container_worker_executable"
  "WORKER_CONTROLLER_TOKEN_FILE_HPCC=$worker_controller_token_file_hpcc"
  "CDL_2010_URL=$cdl_url"
  "SLURM_PARTITION=$slurm_partition"
  "SLURM_ACCOUNT=$slurm_account"
  "SLURM_TIME=$slurm_time"
  "SLURM_MEM=$slurm_mem"
  "SLURM_CPUS_PER_TASK=$slurm_cpus_per_task"
)

remote_prefix=()
for item in "${remote_env[@]}"; do
  remote_prefix+=("$(remote_quote "$item")")
done

remote_script='
set -euo pipefail

controller_health="${CONTROLLER_URL%/}/healthz"

echo "HPCC dev-node context"
echo "hostname=$(hostname)"
echo "user=$(whoami)"
echo "pwd=$(pwd)"

echo
echo "checking HPCC commands"
command -v curl
command -v sbatch
if command -v singularity >/dev/null 2>&1; then
  singularity --version
elif command -v apptainer >/dev/null 2>&1; then
  apptainer --version
else
  echo "missing singularity or apptainer" >&2
  exit 1
fi
command -v 7z || command -v 7za || command -v 7zr

echo
echo "checking HPCC paths"
mkdir -p "$HPCC_SCRATCH_ROOT/runtime/data" "$HPCC_SCRATCH_ROOT/runtime/tmp" "$HPCC_SCRATCH_ROOT/runtime/logs" "$HPCC_SCRATCH_ROOT/cache" "$HPCC_SCRATCH_ROOT/source" "$PUBLISH_ROOT"
test -d "$HPCC_SCRATCH_ROOT"
test -d "$LANDCORE_DATA_ROOT"
test -r "$YANROY_H18V07_PATH"
test -r "${YANROY_H18V07_PATH}.hdr"
test -r "$YANROY_RELEASE_ARCHIVE"
test -w "$PUBLISH_ROOT"
test -s "$WORKER_CONTROLLER_TOKEN_FILE_HPCC"
test -r "$WORKER_CONTROLLER_TOKEN_FILE_HPCC"
df -h "$HPCC_SCRATCH_ROOT" "$LANDCORE_DATA_ROOT" "$PUBLISH_ROOT"

echo
echo "checking controller HTTPS from HPCC dev node"
curl --fail --silent --show-error --max-time 20 "$controller_health" >/dev/null

echo
echo "checking CDL download endpoint from HPCC dev node"
curl --fail --silent --show-error --location --head --max-time 30 "$CDL_2010_URL" >/dev/null

echo
echo "checking controller HTTPS from Slurm compute node"
slurm_args=(--wait --parsable "--time=$SLURM_TIME" "--mem=$SLURM_MEM" "--cpus-per-task=$SLURM_CPUS_PER_TASK")
if [[ -n "$SLURM_PARTITION" ]]; then
  slurm_args+=("--partition=$SLURM_PARTITION")
fi
if [[ -n "$SLURM_ACCOUNT" ]]; then
  slurm_args+=("--account=$SLURM_ACCOUNT")
fi
slurm_dir="$HPCC_SCRATCH_ROOT/preflight"
mkdir -p "$slurm_dir"
slurm_out="$slurm_dir/slurm-controller-https-$(date +%Y%m%d%H%M%S).out"
sbatch "${slurm_args[@]}" --output="$slurm_out" --wrap="hostname; command -v curl; curl --fail --silent --show-error --max-time 20 '"'"'$controller_health'"'"' >/dev/null; echo compute_controller_https_ok"
cat "$slurm_out"
grep -q "compute_controller_https_ok" "$slurm_out"

echo
echo "checking worker image runtime dependencies"
container_runtime="$(command -v singularity || command -v apptainer)"
binds="$HPCC_SCRATCH_ROOT:$HPCC_SCRATCH_ROOT,$LANDCORE_DATA_ROOT:$LANDCORE_DATA_ROOT"
"$container_runtime" exec --bind "$binds" "$GORC_GDAL_IMAGE" sh -lc "
set -e
if [ -x \"\$CONTAINER_GOET_WORKER_EXECUTABLE\" ]; then
  printf '\''worker executable: %s\n'\'' \"\$CONTAINER_GOET_WORKER_EXECUTABLE\"
elif command -v goetl-worker >/dev/null 2>&1; then
  command -v goetl-worker
else
  echo '\''missing goetl-worker in container'\'' >&2
  exit 1
fi
command -v goet-geospatial || { echo '\''missing goet-geospatial in container PATH'\'' >&2; exit 1; }
command -v python3 || { echo '\''missing python3 in container'\'' >&2; exit 1; }
command -v gdalinfo || { echo '\''missing gdalinfo in container'\'' >&2; exit 1; }
gdalinfo --version
python3 - <<'\''PY'\''
try:
    from osgeo import gdal
except Exception as exc:
    raise SystemExit(f\"missing Python osgeo.gdal in container: {exc}\")
try:
    import numpy
except Exception as exc:
    raise SystemExit(f\"missing numpy in container: {exc}\")
print(\"python_gdal_ok\")
print(\"numpy_ok\")
PY
command -v 7z || command -v 7za || command -v 7zr || { echo '\''missing 7z/7za/7zr in container'\'' >&2; exit 1; }
"

echo
echo "HPCC preflight passed"
'

"$ssh_bin" -o BatchMode=yes -o ConnectTimeout=20 "$hpcc_ssh_host" "env ${remote_prefix[*]} bash -s" <<<"$remote_script"
