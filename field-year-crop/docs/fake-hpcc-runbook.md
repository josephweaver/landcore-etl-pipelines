# Fake HPCC Runbook

Run the OS-006 fake HPCC smoke from the repository root:

```bash
bash field-year-crop/scripts/smoke/fake_hpcc_field_crop_year_2010.sh
```

The smoke stages a 3x3 synthetic field raster and CDL raster under
`/tmp/landcore-field-year-crop/fake-hpcc-synthetic`, starts the sibling
`../go-etl` controller with a local Slurm execution environment, and places
`../go-etl/scripts/fake-hpcc` first on `PATH` so `sbatch` resolves to the fake
Slurm shim.

The workflow JSON is generated under `field-year-crop/.run/...` for the run and
submitted inline with its `source_manifest` files. This keeps OS-006 from
adding a tracked workflow fork while preserving the same count-and-summary
product logic used by OS-005.

Expected outputs:

- `field_crop_year_counts.csv`
- `field_crop_year_counts.metadata.json`
- `field_crop_year_summary.csv`
- `field_crop_year_summary.metadata.json`
- fake Slurm logs under `field-year-crop/.run/fake-hpcc-synthetic/tmp/<run>/fake-slurm`
- worker runtime logs under `/tmp/landcore-field-year-crop/fake-hpcc-synthetic/runtime/logs`

The script uses `/tmp/landcore-field-year-crop/go-build-cache` for Go build
cache and `/tmp/landcore-field-year-crop/goetl-bin` for reusable go-etl
binaries. Delete those directories to force a clean rebuild.
