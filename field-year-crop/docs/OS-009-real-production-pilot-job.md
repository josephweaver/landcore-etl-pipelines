# OS-009: Real Production Pilot Job

Status: Verified
Scope: LandCore repository first, with deployment of already-implemented GORC runtime features allowed
Do not modify: GORC core, GORC worker, GORC controller, GORC data-asset providers, GORC geospatial plugins unless a new blocker is recorded and explicitly accepted

Recommended model: 5.4
Recommended reasoning: high

## Purpose

Replace the OS-008 planning-only production template with a real, submit-ready
production pilot job that runs on the Google VM controller and HPCC Slurm worker
environment.

OS-008 created packaging, delivery manifests, and a tiny dry run. It did not
create a real production workflow because the tracked
`production-field-crop-year.workflow.json` is a LandCore planning envelope, not
a canonical GORC workflow. OS-009 should create and run the first production
pilot that uses the current GORC features:

```text
canonical Workflow document
list.crossproduct(years, tiles)
explicit cache_data inputs
dependency-aware staged work
HPCC Slurm worker scale-out through the caretaker
delivery package generation
optional commit_data publication to gdrive_rclone
```

This slice is successful only when a real HPCC submission completes and produces
reviewable field-crop-year outputs for more than one work unit.

## Current State

Verified inputs and runtime facts:

```text
Google VM controller endpoint: https://34-10-225-164.sslip.io
HPCC SSH alias: dev-amd20.passwordless
HPCC scratch root: /mnt/scratch/weave151/etl
Yan/Roy extracted root: /mnt/scratch/weave151/data
Known real tile path: /mnt/scratch/weave151/data/h18v07/WELD_h18v07_2010_field_segments
Known h23v08 header path: /mnt/scratch/weave151/data/h23v08/WELD_h23v08_2010_field_segments.hdr
GDAL worker image: /mnt/scratch/weave151/etl/runtime/images/goetl-worker-gdal-os007.sif
```

OS-007 proved a real one-tile HPCC run:

```text
year = 2010
tile = h18v07
submission = run-fe4b978addfb854ad644b2c2ba899dca
completed work items = 4
validation status = passed
```

OS-008 proved package mechanics only:

```text
merge_field_crop_year_outputs.py
write_delivery_manifest.py
write_gdrive_publish_plan.py
validate_delivery_package.py
production_dry_run.sh
```

New GORC capabilities available for this slice:

```text
canonical object-form workflow variables
semantic function variables with variable.DefaultFunctionRegistry()
list.crossproduct(left, right)
canonical data.inputs / data.outputs model
explicit cache_data and commit_data steps
gdrive_rclone input and output targets
Slurm worker time directives
caretaker worker scale-out with worker_max_active > 1
```

## Target State

A tracked, submit-ready production pilot workflow exists under
`field-year-crop/workflows/` and a matching rendered-submission template exists
under `field-year-crop/submissions/`.

The pilot uses explicit `years` and `tiles` lists, derives year-tile work units
with `list.crossproduct`, runs the real raster path for each pair, merges the
outputs, writes delivery evidence, and publishes or stages a delivery package.

The first pilot should be small enough to inspect but large enough to prove
production mechanics:

```text
default years: 2010
CDL production year range: 2008-2023 inclusive
default tiles: h18v07, h23v08
minimum work units: 2
preferred expansion if inputs are confirmed: years 2010, 2011 x tiles h18v07, h23v08
```

Use h23v08 only after a preflight confirms both the raster data file and `.hdr`
are present and readable inside the worker image. If h23v08 is not readable,
use another SC-approved tile from `SC-landcore-field-crop-year-data-product.md`
that is present under `/mnt/scratch/weave151/data`.

## Allowed Files

```text
land-core.project.json
field-year-crop/workflows/production-pilot-field-crop-year.workflow.json
field-year-crop/submissions/production-pilot-field-crop-year.template.json
field-year-crop/configs/hpcc-controller.template.json
field-year-crop/configs/hpcc-worker.template.json
field-year-crop/configs/hpcc-worker-gdrive.template.json
field-year-crop/scripts/python/render_production_pilot_submission.py
field-year-crop/scripts/python/merge_field_crop_year_outputs.py
field-year-crop/scripts/python/write_delivery_manifest.py
field-year-crop/scripts/python/write_gdrive_publish_plan.py
field-year-crop/scripts/python/validate_delivery_package.py
field-year-crop/scripts/smoke/production_pilot_preflight.sh
field-year-crop/scripts/smoke/production_pilot_hpcc.sh
field-year-crop/docs/production-runbook.md
field-year-crop/docs/runbook.md
field-year-crop/docs/STATE.md
field-year-crop/docs/issues.md
```

Do not overwrite OS-007's proven one-tile workflow. Keep it as the regression
fixture. Do not keep using OS-008's `ProductionWorkflowTemplate` as the pilot
workflow; create a canonical GORC workflow for the real job.

## Canonical Workflow Requirements

The pilot workflow must use GORC canonical syntax:

```json
{
  "api_version": "goet/v1alpha1",
  "kind": "Workflow",
  "id": "production-pilot-field-crop-year"
}
```

It must carry explicit lists and derive work units through `list.crossproduct`:

```json
{
  "variables": {
    "years": [2010],
    "tiles": ["h18v07", "h23v08"],
    "year_tile_pairs": {
      "$type": "list",
      "$call": "list.crossproduct",
      "$args": [
        {"$ref": "years"},
        {"$ref": "tiles"}
      ]
    }
  }
}
```

The implementation may add richer per-tile or per-year metadata maps when paths
cannot be derived safely from templates. Keep the top-level pilot unit list
small and explicit. Do not discover production work by scanning directories at
runtime.

## Data Inputs

Use real HPCC-visible inputs for the first pilot:

```text
Yan/Roy tile rasters: registered_location or local_file paths under /mnt/scratch/weave151/data/<tile>
CDL year source: HTTP ZIP or pre-staged HPCC source path under /mnt/scratch/weave151/etl/source
```

The preferred production-pilot input strategy is:

```text
1. use registered_location for already-extracted Yan/Roy tile rasters;
2. use cache_data for CDL year acquisition and extraction;
3. record a follow-up if the current archive selector still requires a known CDL member path;
4. keep gdrive_rclone Yan/Roy acquisition optional for this slice unless the preflight proves rclone is ready.
```

Reason: this slice is about proving the real production job shape. It should not
block on Google Drive credential setup when OS-007 has already proven the
staged Yan/Roy HPCC path.

If Google Drive input is enabled, the workflow must use `gdrive_rclone` through
configured worker runtime credentials. It must not commit tokens, rendered
rclone config, service account JSON, private Drive paths, or local absolute
credential paths.

## Workflow Stages

The real pilot should use this staged DAG:

```text
cache_data(cdl_year)
cache_data or reference(yanroy_tile)
  -> raster_info(yanroy_tile)
  -> raster_info(cdl_year)
  -> align_to_grid(cdl_year, like_raster=yanroy_tile)
  -> raster_pair_value_counts(yanroy_tile, aligned_cdl_year_tile)
  -> summarize_field_crop_counts(year, tile)
  -> validate_field_crop_year_product(year, tile)
merge year-tile outputs
write delivery_manifest.json
write gdrive_publish_plan.json
validate delivery package
optional commit_data delivery package to gdrive_rclone
```

Fanout policy:

```text
cache_data(cdl_year): one per distinct year
yanroy materialization/reference: one per distinct tile
metadata: one per year or tile input
align_to_grid: one per year-tile pair
raster_pair_value_counts: one per year-tile pair
summarize: one per year-tile pair
validate: one per year-tile pair
merge/package: one per run
commit_data: one per reviewed package, enabled only when the operator requests publication
```

If GORC cannot yet express a required aggregation dependency directly, use the
narrowest renderer or wrapper needed to produce a canonical workflow with
explicit dependencies. Do not fall back to a descriptive planning envelope.

## Work Item Contracts

Reuse existing LandCore scripts and GORC geospatial operations:

```text
run_align_to_grid.py
run_numpy_pair_counts.py or goet-geospatial raster_pair_value_counts
summarize_field_crop_counts.py
validate_field_crop_year_product.py
merge_field_crop_year_outputs.py
write_delivery_manifest.py
write_gdrive_publish_plan.py
validate_delivery_package.py
```

The pilot must preserve these invariants:

```text
align_to_grid uses nearest-neighbor resampling
raster_pair_value_counts uses require_aligned_grid=true
field IDs are not remapped or truncated
output rows include tile as well as year
per-work-unit outputs are deterministic and sorted
merged outputs contain no duplicate field_id, crop_id, year, tile rows
delivery manifest hashes match package files
private source rasters are not copied into the delivery package
```

If h23v08 or another pilot tile contains field IDs above UInt16, the workflow
must use the current UInt32-compatible path that was proven after OS-007. Do not
coerce field IDs to UInt16.

## Publication Policy

The first production pilot must always write a complete delivery package to HPCC
scratch:

```text
/mnt/scratch/weave151/etl/publish/field-crop-year-delivery/<run-id>
```

Google Drive publication is allowed in this slice only as an explicit operator
mode:

```text
publication_mode = plan_only
publication_mode = commit_gdrive
```

`plan_only` writes and validates `gdrive_publish_plan.json` but does not upload.

`commit_gdrive` uses canonical `commit_data` to publish the delivery ZIP through
`gdrive_rclone` under:

```text
gdrive:Data/ETL/tile-field-year-crop/<run-id>/tile-field-year-crop-delivery.zip
```

The human-facing final folder is `/Data/ETL/tile-field-year-crop`. The
configured Google Drive folder ID is `1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4`. It
must record uploaded object evidence. If `commit_data` fails because the remote,
credentials, folder path, or worker image is not configured, record the blocker
and leave the package on HPCC scratch.

Do not run `commit_gdrive` by default.

## Preflight

`production_pilot_preflight.sh` must check:

```text
controller /healthz returns 204
controller uses a GORC binary with canonical function registry support
worker_max_active is greater than 1 for scale-out evidence
HPCC dev node is reachable
Slurm sbatch is available
worker image exists and contains goetl-worker, goet-geospatial, python3, GDAL, numpy, and 7z
HPCC scratch source/cache/publish/log roots exist and are writable
Yan/Roy pilot tile raster and .hdr files exist for every selected tile
gdalinfo succeeds inside the worker image for every selected Yan/Roy tile
CDL source acquisition path is available for every selected year
available scratch space is sufficient for selected years and tiles
publication_mode is either plan_only or commit_gdrive
if publication_mode=commit_gdrive, rclone and gdrive_rclone output config are present
```

The preflight must print selected years, selected tiles, expected pair count,
worker image path, controller URL, and publication mode. It must not print
tokens, rclone config contents, SSH private keys, or bearer tokens.

## Execution Command

The committed smoke command should be:

```bash
bash field-year-crop/scripts/smoke/production_pilot_preflight.sh
bash field-year-crop/scripts/smoke/production_pilot_hpcc.sh
```

`production_pilot_hpcc.sh` should render only ignored local files under
`field-year-crop/.run/production-pilot/<timestamp>/`, submit through `goet
submit`, poll status, and write a compact run report containing:

```text
submission ID
workflow ID
selected years
selected tiles
expected year-tile pair count
known work items
completed work items
failed work items
delivery package root
delivery manifest path
gdrive publish plan path
publication mode
worker start evidence from controller logs when available
```

## Stop Conditions

Stop and record an issue if:

- the candidate workflow is not canonical `goet/v1alpha1`;
- `list.crossproduct` fails at controller admission;
- selected tile paths are missing or unreadable inside the worker image;
- CDL acquisition or extraction cannot produce a GDAL-readable raster;
- `align_to_grid` cannot produce a CDL raster on the Yan/Roy grid;
- `raster_pair_value_counts` reports grid mismatch with `require_aligned_grid=true`;
- any per-work-unit validation fails;
- merged output contains duplicate `(field_id, crop_id, year, tile)` rows;
- delivery manifest hashes do not match files;
- the caretaker never starts more than one worker while multiple work items are queued and `worker_max_active > 1`;
- `commit_gdrive` is requested but the worker cannot write to the configured Google Drive target;
- controlled logs or artifacts contain raw credential values.

## Out Of Scope

- Full production over the complete SC tile list.
- Automatic Google Drive upload in the default mode.
- Native Google Drive API provider work.
- LandCore data catalog registration.
- New GORC work-item types.
- Unreviewed changes to GORC core behavior.
- Storing large rasters or rendered credentialed configs in Git.

## Completion Criteria

- `production-pilot-field-crop-year.workflow.json` is a submit-ready canonical GORC workflow.
- The workflow derives year-tile work from `list.crossproduct`.
- The pilot uses at least two real year-tile work units on HPCC.
- The Google VM controller admits the workflow without `unknown function` or legacy-wrapper errors.
- The HPCC run completes with all selected work units validated.
- The delivery package contains merged counts, merged summary, metadata, validation JSON, `delivery_manifest.json`, and `gdrive_publish_plan.json`.
- The run report records the submission ID and delivery package root.
- Caretaker logs show worker scale-out was evaluated, and more than one worker start was requested or a clear reason is recorded.
- `publication_mode=plan_only` is the default and is verified.
- `publication_mode=commit_gdrive` either succeeds with uploaded-object evidence or records an exact runtime blocker.
- No private credentials, rendered rclone config, bearer tokens, SSH keys, or large source rasters are committed.

## Verified Run

Verified on 2026-07-12 against the Google VM controller and HPCC Slurm worker
environment:

```text
submission = run-dcbf2b84ffb9fc1b49abdeec34960188
years = 2010
tiles = h18v07,h23v08
known work items = 9
completed = 9
failed = 0
publication_mode = plan_only
delivery root = /mnt/scratch/weave151/etl/publish/field-crop-year-delivery/os009-foreground-002
```

Stage results:

```text
align = 2 completed
pair-counts = 2 completed
summarize = 2 completed
validate = 2 completed
package-delivery = 1 completed
```

Delivery validation:

```text
status = passed
work units = 2
merged counts rows = 631857
merged summary rows = 631857
manifest outputs = 18
gdrive publish-plan objects = 18
publication status = planned
```

Caretaker evidence:

```text
worker_start_requested occurred with pending_queued=2
worker_start_confirmed_by_registration reached live_worker_sessions=2
later stages ran with live_worker_sessions=2 and active_capacity_satisfies_claimable_work
```

## Verified Publication Run

Verified on 2026-07-12 after deploying a GORC controller fix for activation of
explicit `commit_data` work items that depend on prior completed stages:

```text
submission = run-b0be8e51b1a7ea6e82a0f55f97738fbd
production run id = os009-gdrive-003
years = 2010
tiles = h18v07,h23v08
known work items = 10
completed = 10
failed = 0
publication_mode = commit_gdrive
delivery root = /mnt/scratch/weave151/etl/publish/field-crop-year-delivery/os009-gdrive-003
published object = gdrive:Data/ETL/tile-field-year-crop/os009-gdrive-003/tile-field-year-crop-delivery.zip
published size = 17494297 bytes
```

Delivery validation:

```text
status = passed
work units = 2
merged counts rows = 631857
merged summary rows = 631857
gdrive publish-plan target_drive_path = Data/ETL/tile-field-year-crop
```

The first two `commit_gdrive` attempts (`os009-gdrive-001` and
`os009-gdrive-002`) completed all compute/package work but failed the publish
stage before queueing the `commit_data` work item:

```text
reason = queue activated stage work items: queued work items are required
```

Root cause: GORC stage activation compiled the explicit `commit_data` work item
with `DependsOn=[package-delivery-delivery]`, but the queue conversion only
queued compiled work items with no dependencies. The fixed controller queues
activation-stage work items whose dependencies are outside the just-compiled
stage, which covers prior-stage dependencies without prematurely queueing
same-stage dependents.
