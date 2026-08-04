# Production Runbook

OS-008 prepares production mechanics after OS-007 verified the external HTTPS
controller and one-tile HPCC run.

## Dry Run

Run the tiny package dry run:

```bash
bash field-year-crop/scripts/smoke/production_dry_run.sh
```

The dry run writes to:

```text
field-year-crop/.run/production-dry-run/delivery
```

It creates two tiny explicit work units, merges counts and summary CSVs, writes
`delivery_manifest.json`, writes `gdrive_publish_plan.json`, and validates the
package. It does not contact HPCC or Google Drive.

## Production Gate

Before full production:

- render `field-year-crop/submissions/production-field-crop-year.template.json`
  with an explicit list of year/tile work units;
- keep the work-unit list limited to `land-core.project.json` scope;
- confirm the active GORC branch applies Slurm resource settings for scale-out;
- run a small HPCC subset first;
- review `delivery_manifest.json` and `gdrive_publish_plan.json`;
- run any Google Drive upload manually from the reviewed plan.

Full production remains manual and human-gated.

## OS-009 Production Pilot

Run the real production pilot against the Google VM controller and HPCC:

```bash
bash field-year-crop/scripts/smoke/production_pilot_preflight.sh
bash field-year-crop/scripts/smoke/production_pilot_hpcc.sh
```

Defaults:

```text
years = 2010,2011
CDL production year range = 2008-2023
tiles = h18v07,h23v08
publication_mode = plan_only
publication_scope = tile_year_summaries
gdrive delivery base path = Data/ETL/tile-field-year-crop
```

The tile allowlist comes from `land-core.project.json`, which mirrors
`../etl/config/projects.yml` for `projects.land_core.vars.tiles_of_interest`.
The CDL year range is 2008-2023 inclusive. CDL ZIP acquisition is part of the
workflow: the `extract-cdl` stage declares `data.inputs.cdl_zip`, GORC plans
deduplicated `cache_data` downloads per selected year, and the worker extracts
the cached ZIP under `$HPCC_SCRATCH_ROOT/source/cdl-<year>`. The HPCC wrapper
only verifies directories and Yan/Roy tile inputs.

Verified plan-only run:

```text
submission = run-dcbf2b84ffb9fc1b49abdeec34960188
delivery root = /mnt/scratch/weave151/etl/publish/field-crop-year-delivery/os009-foreground-002
status = completed
work items = 9 completed, 0 failed
delivery validation = passed
publication status = planned
```

Verified Google Drive publication run:

```bash
PUBLICATION_MODE=commit_gdrive \
PUBLICATION_SCOPE=tile_year_summaries \
GDRIVE_DELIVERY_BASE_PATH=/Data/ETL/tile-field-year-crop \
PRODUCTION_PILOT_RUN_ID=os009-summaries-001 \
bash field-year-crop/scripts/smoke/production_pilot_hpcc.sh
```

```text
submission = run-892b912717a21428b86c703982f82c27
production run id = os009-summaries-001
status = completed
work items = 20 completed, 0 failed
publication_mode = commit_gdrive
publication_scope = tile_year_summaries
publishes one summary CSV per selected year-tile pair:
gdrive:Data/ETL/tile-field-year-crop/h18v07/field_crop_year_summary_2010_h18v07.csv
gdrive:Data/ETL/tile-field-year-crop/h23v08/field_crop_year_summary_2010_h23v08.csv
gdrive:Data/ETL/tile-field-year-crop/h18v07/field_crop_year_summary_2011_h18v07.csv
gdrive:Data/ETL/tile-field-year-crop/h23v08/field_crop_year_summary_2011_h23v08.csv
```

Observed worker behavior: compute stages reached `live_worker_sessions=3`.
The `publish-summaries` stage uploaded one object at a time because current
`commit_data` compilation implicitly applies a per-remote `gdrive_rclone`
upload mutex. That constraint should become an explicit workflow or provider
configuration choice before recurring production use.

Full summary publication run:

```bash
PUBLICATION_MODE=commit_gdrive \
PUBLICATION_SCOPE=tile_year_summaries \
GDRIVE_DELIVERY_BASE_PATH=/Data/ETL/tile-field-year-crop \
PRODUCTION_PILOT_RUN_ID=os009-full-002 \
PILOT_YEARS=2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023 \
PILOT_TILES=<comma-separated tiles_of_interest from land-core.project.json> \
bash field-year-crop/scripts/smoke/production_pilot_hpcc.sh
```

For the current `tiles_of_interest` list, the full run is 16 CDL years by 87
tiles: 1,392 tile-year summaries published as individual CSV files under
`gdrive:Data/ETL/tile-field-year-crop/<tile>/`.

Rclone printed a follow-up warning that the current `gdrive` remote uses
rclone's shared Google Drive client ID, which rclone reports is being retired
during 2026. Configure a project-specific client ID before relying on this
path for recurring production publication.
