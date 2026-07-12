#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

controller_url="${CONTROLLER_URL:-https://34-10-225-164.sslip.io}"
controller_ssh_host="${CONTROLLER_SSH_HOST:-instance-20260710-150616.us-central1-a.gorc-2026-07}"
hpcc_ssh_host="${HPCC_SSH_HOST:-dev-amd20.passwordless}"
ssh_bin="${SSH_BIN:-ssh}"
hpcc_scratch_root="${HPCC_SCRATCH_ROOT:-/mnt/scratch/weave151/etl}"
landcore_data_root="${LANDCORE_DATA_ROOT:-/mnt/scratch/weave151/data}"
worker_image="${GORC_GDAL_IMAGE:-/mnt/scratch/weave151/etl/runtime/images/goetl-worker-gdal-os007.sif}"
geospatial_executable="${GEOSPATIAL_EXECUTABLE:-/goetl/goet-geospatial}"
years_csv="${PILOT_YEARS:-2010}"
tiles_csv="${PILOT_TILES:-h18v07,h23v08}"
publication_mode="${PUBLICATION_MODE:-plan_only}"
gdrive_remote="${GDRIVE_REMOTE:-gdrive}"
gdrive_delivery_base_path="${GDRIVE_DELIVERY_BASE_PATH:-Data/ETL/tile-field-year-crop}"

if [[ "$publication_mode" != "plan_only" && "$publication_mode" != "commit_gdrive" ]]; then
  echo "PUBLICATION_MODE must be plan_only or commit_gdrive, got: $publication_mode" >&2
  exit 2
fi

python3 - "$root_dir/land-core.project.json" "$years_csv" "$tiles_csv" <<'PY'
import json
import re
import sys
from pathlib import Path

project = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
years = [item.strip() for item in sys.argv[2].split(",") if item.strip()]
tiles = [item.strip() for item in sys.argv[3].split(",") if item.strip()]
allowed = set(project.get("tiles_of_interest", []))
if not years:
    raise SystemExit("at least one pilot year is required")
if not tiles:
    raise SystemExit("at least one pilot tile is required")
for year in years:
    parsed = int(year)
    if parsed < 2008 or parsed > 2023:
        raise SystemExit(f"CDL years must be 2008-2023: {parsed}")
for tile in tiles:
    if not re.fullmatch(r"h\d{2}v\d{2}", tile):
        raise SystemExit(f"invalid tile id: {tile}")
missing = [tile for tile in tiles if tile not in allowed]
if missing:
    raise SystemExit(f"pilot tiles are not in land-core.project.json tiles_of_interest: {missing}")
print(f"selected years: {','.join(years)}")
print(f"selected tiles: {','.join(tiles)}")
print(f"expected year-tile pairs: {len(years) * len(tiles)}")
PY

echo "controller url: $controller_url"
echo "publication mode: $publication_mode"
echo "gdrive delivery base path: ${gdrive_delivery_base_path#/}"
curl -fsS -o /dev/null "$controller_url/healthz"
echo "controller healthz: passed"

set +e
worker_max_active="$("$ssh_bin" -o BatchMode=yes -o ConnectTimeout=20 "$controller_ssh_host" "sudo python3 - <<'PY'
import json
from pathlib import Path
payload = json.loads(Path('/etc/gorc/controller.json').read_text(encoding='utf-8-sig'))
print(payload.get('worker_max_active', ''))
PY" 2>/dev/null)"
controller_rc=$?
set -e
if [[ "$controller_rc" -eq 0 && "$worker_max_active" =~ ^[0-9]+$ ]]; then
  echo "controller worker_max_active: $worker_max_active"
  if (( worker_max_active <= 1 )); then
    echo "controller worker_max_active must be greater than 1 for OS-009 scale-out evidence" >&2
    exit 1
  fi
else
  echo "warning: could not inspect controller worker_max_active through $controller_ssh_host" >&2
fi

remote_script='
set -euo pipefail
command -v sbatch >/dev/null
command -v singularity >/dev/null || command -v apptainer >/dev/null
test -d "$HPCC_SCRATCH_ROOT"
mkdir -p "$HPCC_SCRATCH_ROOT/source" "$HPCC_SCRATCH_ROOT/publish" "$HPCC_SCRATCH_ROOT/runtime/logs"
test -w "$HPCC_SCRATCH_ROOT/source"
test -w "$HPCC_SCRATCH_ROOT/publish"
test -r "$WORKER_IMAGE"
runtime="$(command -v singularity || command -v apptainer)"
"$runtime" exec "$WORKER_IMAGE" python3 - <<'"'"'PY'"'"'
import numpy
from osgeo import gdal
print("python-gdal-numpy: ok")
PY
"$runtime" exec "$WORKER_IMAGE" test -x "$GEOSPATIAL_EXECUTABLE"
IFS=, read -r -a years <<< "$PILOT_YEARS"
IFS=, read -r -a tiles <<< "$PILOT_TILES"
for tile in "${tiles[@]}"; do
  raster="$LANDCORE_DATA_ROOT/$tile/WELD_${tile}_2010_field_segments"
  test -r "$raster"
  test -r "${raster}.hdr"
  "$runtime" exec "$WORKER_IMAGE" gdalinfo "$raster" >/dev/null
done
for year in "${years[@]}"; do
  if [[ -z "$year" || "$year" =~ [^0-9] ]]; then
    echo "invalid year: $year" >&2
    exit 2
  fi
  command -v curl >/dev/null
done
df -h "$HPCC_SCRATCH_ROOT"
if [[ "$PUBLICATION_MODE" == "commit_gdrive" ]]; then
  test -x "$HPCC_SCRATCH_ROOT/runtime/bin/rclone"
  test -s "$HPCC_SCRATCH_ROOT/runtime/secrets/rclone.conf"
  "$HPCC_SCRATCH_ROOT/runtime/bin/rclone" --config "$HPCC_SCRATCH_ROOT/runtime/secrets/rclone.conf" lsf "$GDRIVE_REMOTE:${GDRIVE_DELIVERY_BASE_PATH#/}" >/dev/null
  echo "gdrive_rclone publication target is reachable"
fi
echo "hpcc production pilot preflight passed"
'

"$ssh_bin" -o BatchMode=yes -o ConnectTimeout=20 "$hpcc_ssh_host" \
  "env HPCC_SCRATCH_ROOT=$(printf '%q' "$hpcc_scratch_root") LANDCORE_DATA_ROOT=$(printf '%q' "$landcore_data_root") WORKER_IMAGE=$(printf '%q' "$worker_image") GEOSPATIAL_EXECUTABLE=$(printf '%q' "$geospatial_executable") PILOT_YEARS=$(printf '%q' "$years_csv") PILOT_TILES=$(printf '%q' "$tiles_csv") PUBLICATION_MODE=$(printf '%q' "$publication_mode") GDRIVE_REMOTE=$(printf '%q' "$gdrive_remote") GDRIVE_DELIVERY_BASE_PATH=$(printf '%q' "$gdrive_delivery_base_path") bash -s" \
  <<<"$remote_script"

if [[ "$publication_mode" == "commit_gdrive" ]]; then
  echo "commit_gdrive requested; rclone-capable runtime target was checked"
fi

echo "production pilot preflight passed"
