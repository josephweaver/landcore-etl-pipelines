# OS-003: Local Synthetic GORC Workflow

Status: Implemented
Scope: LandCore repository only
Do not modify: GORC core, GORC worker, GORC controller, GORC data-asset providers, GORC geospatial plugins

Recommended model: 5.4-mini
Recommended reasoning: medium-high


## Purpose

Create the first LandCore GORC workflow using tiny synthetic data.

This proves that the LandCore repository can define a GORC project/workflow/submission and run the Python product scripts through GORC locally.

Do not use real CDL or real Yan/Roy data in this slice.

## Allowed Files

```text
land-core.project.json
field-year-crop/workflows/local-synthetic-field-crop-year.workflow.json
field-year-crop/submissions/local-synthetic-field-crop-year.submission.json
field-year-crop/configs/local-controller.json
field-year-crop/configs/local-worker.gdal.json
field-year-crop/scripts/python/run_geospatial_pair_counts.py
field-year-crop/scripts/python/summarize_field_crop_counts.py
field-year-crop/scripts/smoke/local_synthetic_field_crop_year.sh
field-year-crop/tests/fixtures/mini/
field-year-crop/docs/runbook.md
field-year-crop/docs/STATE.md
field-year-crop/docs/issues.md
```

## Synthetic Data

Use tiny files only. The easiest approach is to generate synthetic rasters in the smoke script using GDAL inside the GDAL worker container or local GDAL environment.

Synthetic raster values should match this conceptual grid:

Field raster:

```text
1 1 2
1 2 2
3 3 3
```

CDL raster:

```text
5 5 1
5 1 1
2 2 4
```

Expected counts:

```csv
field_id,crop_id,count
1,5,3
2,1,3
3,2,2
3,4,1
```

Expected year: `2010`.

## Workflow Shape

The workflow should have these compute steps:

```text
step 1: run geospatial pair counts
step 2: summarize field crop year
```

It may use local-file data assets or direct fixture paths. Prefer local-file data assets if the current GORC schema supports them cleanly.

The geospatial compute step should be a `python_script` that invokes the external `goet-geospatial` executable. Do not add a new GORC work-item type.

The project file must preserve the LandCore repository, data catalog,
`states_of_interest`, and pilot `tiles_of_interest` facts introduced by OS-001.
Do not replace it with a generic local-only project identity.

## `run_geospatial_pair_counts.py`

Inputs:

```text
--field-raster <path>
--value-raster <path>
--year <int>
--geospatial-executable <path>
--artifact-dir <path or use GOET_ARTIFACT_DIR>
```

Behavior:

1. Write a `raster_pair_value_counts` request JSON.
2. Invoke `goet-geospatial --request ... --response ...`.
3. Copy or leave generated CSV/metadata under `GOET_ARTIFACT_DIR`.
4. Write `GOET_OUTPUT_JSON` declaring:
   - counts CSV artifact
   - geospatial metadata JSON artifact
   - operation response JSON artifact

## Validation

```bash
bash field-year-crop/scripts/smoke/local_synthetic_field_crop_year.sh
```

The smoke script must verify:

```text
field_crop_year_counts.csv exists
field_crop_year_summary.csv exists
summary CSV exactly matches expected rows
GOET/GORC submission completed without failed work
```

## Stop Conditions

Stop and append to `field-year-crop/docs/issues.md` if:

- current GORC workflow JSON cannot express the required source manifest/data assets;
- current GORC local worker config cannot point to the GDAL worker executable;
- `goet-geospatial` is not available in the local runtime.

Do not patch GORC in this slice.

## Completion Criteria

- Local synthetic workflow runs end to end.
- Published/local artifacts are deterministic.
- Runbook documents exact local command.
- No real private data is committed.
- No GORC repo files are modified.
