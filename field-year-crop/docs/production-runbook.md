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
years = 2010
CDL production year range = 2008-2023
tiles = h18v07,h23v08
publication_mode = plan_only
gdrive delivery base path = Data/ETL/tile-field-year-crop
```

The tile allowlist comes from `land-core.project.json`, which mirrors
`../etl/config/projects.yml` for `projects.land_core.vars.tiles_of_interest`.

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
GDRIVE_DELIVERY_BASE_PATH=/Data/ETL/tile-field-year-crop \
PRODUCTION_PILOT_RUN_ID=os009-gdrive-003 \
bash field-year-crop/scripts/smoke/production_pilot_hpcc.sh
```

```text
submission = run-b0be8e51b1a7ea6e82a0f55f97738fbd
delivery root = /mnt/scratch/weave151/etl/publish/field-crop-year-delivery/os009-gdrive-003
status = completed
work items = 10 completed, 0 failed
delivery validation = passed
published object = gdrive:Data/ETL/tile-field-year-crop/os009-gdrive-003/tile-field-year-crop-delivery.zip
published size = 17494297 bytes
```
