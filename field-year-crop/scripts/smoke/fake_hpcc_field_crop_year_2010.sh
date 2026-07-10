#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "$script_dir/../../.." && pwd)"
goetl_dir="$(cd "$root_dir/../go-etl" && pwd)"
synthetic_root="/tmp/landcore-field-year-crop/fake-hpcc-synthetic"
runtime_root="$synthetic_root/runtime"
tmp_parent="$root_dir/field-year-crop/.run/fake-hpcc-synthetic/tmp"
cache_root="${LANDCORE_GO_BUILD_CACHE:-/tmp/landcore-field-year-crop/go-build-cache}"
build_bin_root="${LANDCORE_GOETL_BIN_CACHE:-/tmp/landcore-field-year-crop/goetl-bin}"
tmp_root=""
runner_root="$tmp_root/runner"
bin_dir="$tmp_root/bin"
go_tmp_dir="$cache_root/tmp"
go_cache_dir="$cache_root/cache"
controller_log="$tmp_root/controller.log"
demo_log="$tmp_root/demo.log"
last_status_log="$tmp_root/last-status.log"
workflow_file="$tmp_root/fake-hpcc-synthetic-field-crop-year.workflow.json"
summary_wrapper="$tmp_root/summarize_with_completion_delay.py"
config_root="$tmp_root/configs"
controller_config="$config_root/controller/controller.json"
slurm_run_root="$tmp_root/fake-slurm"
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

for command_name in go gdal_translate gdalinfo python3; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "missing required command: $command_name" >&2
    exit 1
  fi
done

rm -rf "$synthetic_root"
mkdir -p "$tmp_parent" "$synthetic_root"
tmp_root="$(mktemp -d "$tmp_parent/os006.XXXXXX")"
runner_root="$tmp_root/runner"
bin_dir="$tmp_root/bin"
go_tmp_dir="$cache_root/tmp"
go_cache_dir="$cache_root/cache"
controller_log="$tmp_root/controller.log"
demo_log="$tmp_root/demo.log"
last_status_log="$tmp_root/last-status.log"
workflow_file="$tmp_root/fake-hpcc-synthetic-field-crop-year.workflow.json"
summary_wrapper="$tmp_root/summarize_with_completion_delay.py"
config_root="$tmp_root/configs"
controller_config="$config_root/controller/controller.json"
slurm_run_root="$tmp_root/fake-slurm"

mkdir -p "$runtime_root" "$runner_root" "$bin_dir" \
  "$go_tmp_dir" "$go_cache_dir" "$build_bin_root" "$config_root/controller" "$slurm_run_root"
ln -s "$goetl_dir" "$runner_root/go-etl"

build_go_binary() {
  local package_path="$1"
  local output_path="$2"
  local cached_path="$build_bin_root/$(basename "$output_path")"

  if [[ ! -x "$cached_path" ]] || find "$goetl_dir" -path "$goetl_dir/.git" -prune -o -name '*.go' -newer "$cached_path" -print -quit | grep -q .; then
    go build -o "$cached_path" "$package_path"
  fi
  cp "$cached_path" "$output_path"
  chmod +x "$output_path"
}

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

summary_wrapper_rel="$(realpath --relative-to="$root_dir" "$summary_wrapper")"
cat >"$summary_wrapper" <<'PY'
#!/usr/bin/env python3
from __future__ import annotations

import sys
import time
from pathlib import Path


for parent in Path(__file__).resolve().parents:
    scripts_dir = parent / "field-year-crop" / "scripts" / "python"
    if scripts_dir.exists():
        sys.path.insert(0, str(scripts_dir))
        break
else:
    raise SystemExit("failed to locate field-year-crop/scripts/python")

from summarize_field_crop_counts import main


exit_code = main()
time.sleep(3)
raise SystemExit(exit_code)
PY

cat >"$workflow_file" <<'EOF'
{
  "workflow": {
    "ID": "fake-hpcc-synthetic-field-crop-year",
    "Variables": [
      {
        "name": {
          "namespace": "workflow",
          "key": "pair_runs"
        },
        "type": "list",
        "expression": [
          {
            "type": "object",
            "expression": {
              "id": {
                "type": "string",
                "expression": "2010"
              },
              "python_entrypoint": {
                "type": "path",
                "expression": "field-year-crop/scripts/python/run_numpy_pair_counts.py"
              },
              "python_args": {
                "type": "list",
                "expression": [
                  {
                    "type": "string",
                    "expression": "--field-raster=/tmp/landcore-field-year-crop/fake-hpcc-synthetic/field.tif"
                  },
                  {
                    "type": "string",
                    "expression": "--value-raster=/tmp/landcore-field-year-crop/fake-hpcc-synthetic/cdl.tif"
                  },
                  {
                    "type": "string",
                    "expression": "--year=2010"
                  },
                  {
                    "type": "string",
                    "expression": "--counts-csv=/tmp/landcore-field-year-crop/fake-hpcc-synthetic/field_crop_year_counts.csv"
                  },
                  {
                    "type": "string",
                    "expression": "--metadata-json=/tmp/landcore-field-year-crop/fake-hpcc-synthetic/field_crop_year_counts.metadata.json"
                  }
                ]
              }
            }
          }
        ]
      },
      {
        "name": {
          "namespace": "workflow",
          "key": "summary_runs"
        },
        "type": "list",
        "expression": [
          {
            "type": "object",
            "expression": {
              "id": {
                "type": "string",
                "expression": "2010"
              },
              "python_entrypoint": {
                "type": "path",
                "expression": "field-year-crop/scripts/python/summarize_field_crop_counts.py"
              },
              "python_args": {
                "type": "list",
                "expression": [
                  {
                    "type": "string",
                    "expression": "--counts-csv=/tmp/landcore-field-year-crop/fake-hpcc-synthetic/field_crop_year_counts.csv"
                  },
                  {
                    "type": "string",
                    "expression": "--year=2010"
                  },
                  {
                    "type": "string",
                    "expression": "--output-csv=/tmp/landcore-field-year-crop/fake-hpcc-synthetic/field_crop_year_summary.csv"
                  },
                  {
                    "type": "string",
                    "expression": "--metadata-json=/tmp/landcore-field-year-crop/fake-hpcc-synthetic/field_crop_year_summary.metadata.json"
                  }
                ]
              }
            }
          }
        ]
      }
    ],
    "Steps": [
      {
        "ID": "pair-counts",
        "FanOut": {
          "WorkItem": {
            "FanOutExpression": "${pair_runs[*]}",
            "IDTokenAccessor": ".id",
            "OutputAccessor": ".id",
            "Type": "python_script",
            "IDPrefix": "fake_hpcc_field_crop_year_counts",
            "OutputPrefix": "fake_hpcc_field_crop_year_counts",
            "OutputExtension": ".json",
            "Parameters": {
              "python_entrypoint": {
                "type": "path",
                "value": "field-year-crop/scripts/python/run_numpy_pair_counts.py"
              },
              "python_args": {
                "type": "list",
                "value": [
                  "unused"
                ]
              }
            },
            "ParameterAccessors": {
              "python_entrypoint": ".python_entrypoint",
              "python_args": ".python_args"
            }
          }
        }
      },
      {
        "ID": "summarize",
        "FanOut": {
          "WorkItem": {
            "FanOutExpression": "${summary_runs[*]}",
            "IDTokenAccessor": ".id",
            "OutputAccessor": ".id",
            "Type": "python_script",
            "IDPrefix": "fake_hpcc_field_crop_year_summary",
            "OutputPrefix": "fake_hpcc_field_crop_year_summary",
            "OutputExtension": ".json",
            "Parameters": {
              "python_entrypoint": {
                "type": "path",
                "value": "field-year-crop/scripts/python/summarize_field_crop_counts.py"
              },
              "python_args": {
                "type": "list",
                "value": [
                  "unused"
                ]
              }
            },
            "ParameterAccessors": {
              "python_entrypoint": ".python_entrypoint",
              "python_args": ".python_args"
            }
          }
        }
      }
    ]
  },
  "source_manifest": {
    "files": [
      {
        "role": "python_entrypoint",
        "path": "field-year-crop/scripts/python/run_numpy_pair_counts.py",
        "content_type": "text/x-python"
      },
      {
        "role": "python_entrypoint",
        "path": "field-year-crop/scripts/python/summarize_field_crop_counts.py",
        "content_type": "text/x-python"
      },
      {
        "role": "support_file",
        "path": "field-year-crop/scripts/python/field_crop_common.py",
        "content_type": "text/x-python"
      }
    ]
  },
  "variables": [
    {
      "name": {
        "namespace": "worker_config",
        "key": "worker_count_per_start"
      },
      "type": "int",
      "expression": 1
    },
    {
      "name": {
        "namespace": "worker_config",
        "key": "worker_max_count"
      },
      "type": "int",
      "expression": 1
    },
    {
      "name": {
        "namespace": "worker_config",
        "key": "worker_min_elapsed_time_between_starts"
      },
      "type": "string",
      "expression": "0s"
    }
  ]
}
EOF

python3 - "$workflow_file" "$summary_wrapper_rel" <<'PY'
import json
import sys
from pathlib import Path

workflow_path = Path(sys.argv[1])
summary_wrapper = sys.argv[2]
document = json.loads(workflow_path.read_text(encoding="utf-8"))

document["workflow"]["Variables"][1]["expression"][0]["expression"]["python_entrypoint"]["expression"] = summary_wrapper
document["workflow"]["Steps"][1]["FanOut"]["WorkItem"]["Parameters"]["python_entrypoint"]["value"] = summary_wrapper
document["source_manifest"]["files"].append(
    {
        "role": "python_entrypoint",
        "path": summary_wrapper,
        "content_type": "text/x-python",
    }
)

workflow_path.write_text(json.dumps(document, indent=2, sort_keys=False) + "\n", encoding="utf-8")
PY

cd "$runner_root/go-etl"
export GOTMPDIR="$go_tmp_dir"
export GOCACHE="$go_cache_dir"
build_go_binary ./cmd/controller "$bin_dir/controller"
build_go_binary ./cmd/worker "$bin_dir/worker"
build_go_binary ./cmd/demo-client "$bin_dir/demo-client"
export PATH="$goetl_dir/scripts/fake-hpcc:$bin_dir:$PATH"
export FAKE_SLURM_RUN_ROOT="$slurm_run_root"
unset FAKE_SLURM_FOREGROUND

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
    "name": "fake-hpcc-synthetic-local-slurm",
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
      "type": "slurm"
    },
    "runtime": {
      "type": "worker",
      "settings": {
        "root": "$runtime_root",
        "controller_url": "http://localhost:8080",
        "local_worker_artifact": "$bin_dir/worker",
        "data_dir": "$runtime_root/data",
        "max_asset_bytes": 20000000000
      }
    }
  }
}
EOF

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
  python3 - "http://localhost:8080" \
    "land-core.project.json" \
    "$workflow_file" <<'PY'
import base64
import json
import sys
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

controller_url = sys.argv[1].rstrip("/")
project_path = Path(sys.argv[2])
workflow_path = Path(sys.argv[3])

project = json.loads(project_path.read_text(encoding="utf-8"))
workflow = json.loads(workflow_path.read_text(encoding="utf-8"))
files = []
for item in workflow.get("source_manifest", {}).get("files", []):
    source_path = Path(item["path"])
    files.append({
        "path": item["path"],
        "content": base64.b64encode(source_path.read_bytes()).decode("ascii"),
    })

payload = json.dumps({
    "project": project,
    "workflow": workflow,
    "files": files,
}).encode("utf-8")

request = Request(
    f"{controller_url}/workflow",
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST",
)
try:
    with urlopen(request, timeout=600) as response:
        body = response.read().decode("utf-8")
except HTTPError as error:
    body = error.read().decode("utf-8", errors="replace")
    raise SystemExit(f"submit workflow: unexpected status {error.code}: {body}")

ack = json.loads(body)
print(f"Submission: {ack['submission_id']}")
print(f"Workflow: {ack['workflow_id']}")
print(f"Initial work items: {ack['initial_work_item_count']}")
PY
) >"$demo_log" 2>&1

submission_id="$(sed -n 's/^Submission: //p' "$demo_log" | tail -n 1)"
if [[ -z "$submission_id" ]]; then
  echo "failed to read submission ID from demo-client output" >&2
  cat "$demo_log" >&2 || true
  exit 1
fi

wait_deadline=$((SECONDS + 600))
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

  sleep 1
done

if ! grep -q '^Status: completed$' "$last_status_log"; then
  echo "timed out waiting for workflow completion" >&2
  cat "$last_status_log" >&2 || true
  exit 1
fi

counts_csv="$synthetic_root/field_crop_year_counts.csv"
counts_metadata="$synthetic_root/field_crop_year_counts.metadata.json"
summary_csv="$synthetic_root/field_crop_year_summary.csv"
summary_metadata="$synthetic_root/field_crop_year_summary.metadata.json"
worker_config="$runtime_root/config/worker.json"
worker_script="$runtime_root/scripts/worker.slurm"
worker_log="$runtime_root/logs/worker.log"
submissions_log="$slurm_run_root/submissions.log"

for path in "$counts_csv" "$counts_metadata" "$summary_csv" \
  "$summary_metadata" "$worker_config" "$worker_script" "$worker_log" "$submissions_log"; do
  if [[ ! -f "$path" ]]; then
    echo "missing expected fake-HPCC artifact: $path" >&2
    cat "$demo_log" >&2
    exit 1
  fi
done

python3 - "$counts_csv" "$summary_csv" "$counts_metadata" "$summary_metadata" "$submissions_log" "$worker_script" "$slurm_run_root" <<'PY'
import json
import sys
from pathlib import Path

counts_csv = Path(sys.argv[1])
summary_csv = Path(sys.argv[2])
counts_metadata = Path(sys.argv[3])
summary_metadata = Path(sys.argv[4])
submissions_log = Path(sys.argv[5])
worker_script = Path(sys.argv[6])
slurm_run_root = Path(sys.argv[7])

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

if counts_csv.read_text(encoding="utf-8") != expected_counts:
    raise SystemExit("counts CSV mismatch")
if summary_csv.read_text(encoding="utf-8") != expected_summary:
    raise SystemExit("summary CSV mismatch")

counts_meta = json.loads(counts_metadata.read_text(encoding="utf-8"))
summary_meta = json.loads(summary_metadata.read_text(encoding="utf-8"))
submissions = submissions_log.read_text(encoding="utf-8")
script_text = worker_script.read_text(encoding="utf-8")

if counts_meta["valid_pixels"] != 9 or counts_meta["distinct_fields"] != 3 or counts_meta["distinct_pairs"] != 4:
    raise SystemExit("counts metadata mismatch")
if summary_meta["year"] != 2010 or summary_meta["output_row_count"] != 4:
    raise SystemExit("summary metadata mismatch")
if counts_meta["method"] != "numpy_unique_uint64_pair_key":
    raise SystemExit("unexpected pair-count method")
if submissions.count("job_id=") < 1:
    raise SystemExit("expected fake Slurm to submit at least one worker job")
if "#SBATCH --job-name=goetl-worker" not in script_text:
    raise SystemExit("worker script did not use fake-HPCC Slurm job name")
if not any(slurm_run_root.glob("job-*.out")):
    raise SystemExit("missing fake Slurm job stdout log")
PY

if ! grep -q 'Status: completed' "$demo_log" || ! grep -q 'Failed: 0' "$demo_log"; then
  echo "workflow did not complete cleanly" >&2
  cat "$demo_log" >&2
  exit 1
fi

echo "fake HPCC synthetic field-crop-year workflow completed"
