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
