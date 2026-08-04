# Field-Crop-Year Runbook

This runbook captures the staged execution path:

1. local debug orchestration
2. fake HPCC run
3. real HPCC preflight and scale-out

## Local Debug

- Validate `land-core.project.json` is present.
- Ensure `field-year-crop/docs/data-product-spec.md` matches the expected output schema.
- Run the local synthetic/fixture workflow before first real input work:
  `bash field-year-crop/scripts/smoke/local_synthetic_field_crop_year.sh`
- The local smoke uses the sibling `../go-etl` checkout under WSL to build the controller,
  worker, geospatial executable, and demo client. It starts the controller, submits
  `land-core.project.json` and the local synthetic workflow through `goet submit`, and
  relies on the workflow `source_manifest` to package the Python source files.
- Controller logs land in the smoke temp directory as `controller.log`; controller-owned
  worker logs land under `controller-runtime/logs/worker.log`, with attempt logs under
  `controller-runtime/tmp/attempts/<attempt-id>/logs/`.
- Ensure the repo-local Yan/Roy tile exists at
  `.data/h18v07/WELD_h18v07_2010_field_segments.hdr` with matching sidecars, or keep
  `.data/ReleaseData.7z` available with a local `7z`/`7za`/`7zr` executable. Then run
  the real-input metadata smoke:
  `bash field-year-crop/scripts/smoke/local_real_input_metadata_2010.sh`
- The real-input metadata smoke downloads the 2010 CDL ZIP to
  `/tmp/landcore-field-year-crop/local-real-input-metadata-2010/source`, extracts it
  under `/tmp/landcore-field-year-crop/local-real-input-metadata-2010/cdl-2010`,
  stages Yan/Roy h18v07 under
  `/tmp/landcore-field-year-crop/local-real-input-metadata-2010/yanroy`, and submits
  `field-year-crop/workflows/local-real-input-metadata-2010.workflow.json`.
- For large local runs, the real-input smokes keep the workflow-visible path
  `/tmp/landcore-field-year-crop` stable but store it on `LANDCORE_TMP_ROOT` when
  set, or `/mnt/d/landcore-tmp` when `/mnt/d` is mounted and writable. Set
  `LANDCORE_TMP_ROOT=/tmp` to force WSL-local storage.
- Metadata outputs land under
  `/tmp/landcore-field-year-crop/local-real-input-metadata-2010/metadata`:
  `cdl_2010_raster_info.json`, `yanroy_h18v07_raster_info.json`, and
  `input_discovery.json`.
- Run the real local product workflow with:
  `bash field-year-crop/scripts/smoke/local_field_crop_year_2010.sh`
- The real product smoke stages the same Yan/Roy h18v07 input, reuses or extracts
  the CDL ZIP, submits `field-year-crop/workflows/local-field-crop-year-2010.workflow.json`,
  and writes outputs under `/tmp/landcore-field-year-crop/local-field-crop-year-2010`.
- Expected OS-005 product artifacts are:
  `metadata/input_discovery.json`, `metadata/raster_info.json`,
  `aligned/cdl_2010_on_h18v07_grid.tif`,
  `aligned/cdl_2010_on_h18v07_grid.metadata.json`,
  `counts/field_crop_counts_2010.csv`,
  `counts/field_crop_counts_2010.metadata.json`,
  `summary/field_crop_year_summary_2010.csv`,
  `summary/field_crop_year_summary_2010.metadata.json`, and
  `validation/field_crop_year_validation_2010.json`.
- OS-005 can reuse roughly 9.3 GB from the OS-004 CDL download/extraction. It also
  creates a tile-sized aligned CDL GeoTIFF and a tile-sized UInt32 Yan/Roy working
  raster under the OS-005 runtime root.
- Verified OS-005 h18v07 2010 output on the D-backed runtime:
  `D:\landcore-tmp\landcore-field-year-crop\local-field-crop-year-2010`.
  Validation passed with 221,084 count rows, 221,084 summary rows, 47,922
  distinct fields, 20,902,248 counted pixels, and max `field_id` 47,922. The
  Numpy pair-count worker reported 7.731 seconds for pair aggregation.

## Fake HPCC

- Run the fake HPCC synthetic graduation smoke:
  `bash field-year-crop/scripts/smoke/fake_hpcc_field_crop_year_2010.sh`
- The smoke stages a tiny 3x3 synthetic field/CDL pair under
  `/tmp/landcore-field-year-crop/fake-hpcc-synthetic`, starts the sibling
  `../go-etl` controller with a local Slurm execution environment, and uses
  `../go-etl/scripts/fake-hpcc/sbatch` as the fake Slurm scheduler.
- The workflow JSON is generated under `field-year-crop/.run/...` and submitted
  inline with a `source_manifest`; no tracked fake workflow fork is required.
- Expected fake HPCC outputs are `field_crop_year_counts.csv`,
  `field_crop_year_counts.metadata.json`, `field_crop_year_summary.csv`, and
  `field_crop_year_summary.metadata.json` under
  `/tmp/landcore-field-year-crop/fake-hpcc-synthetic`.
- Fake Slurm submission logs land under
  `field-year-crop/.run/fake-hpcc-synthetic/tmp/<run>/fake-slurm`.
- Verified OS-006 on 2026-07-10. The warmed-cache run completed successfully in
  about 208 seconds on this WSL setup.

## Real HPCC

- Start with the one-tile HPCC preflight in `field-year-crop/docs/hpcc-runbook.md`.
- Target `dev-amd20.passwordless` with `/mnt/scratch/weave151/etl` as the base
  operations root and `/mnt/scratch/weave151/data` as the staged/extracted
  Yan/Roy root. The pilot h18v07 header is
  `/mnt/scratch/weave151/data/h18v07/WELD_h18v07_2010_field_segments`
  with its companion `.hdr`; `/mnt/scratch/weave151/data/ReleaseData.7z` remains available as the source
  archive.
- Use explicit Slurm resource fields for OS-007 and run workers through
  Singularity/Apptainer.
- Use the external HTTPS controller path for OS-007. The controller runs on a
  Google VM or equivalent dedicated server, reaches HPCC through GORC
  `jump_hosts`, and workers call back over HTTPS 443 with a worker token file.
  Laptop reverse-tunnel controller paths are development-only.
- Download CDL 2010 on HPCC into scratch; do not use the small HPCC home
  directory for downloads, extraction, runtime data, or publication.
- Publish OS-007 output to the HPCC scratch filesystem only. Google Drive is a
  fast follower after the real HPCC one-tile path is stable.
- Keep output credentials out of this repository; use runtime environment injection.

## Production Packaging

- Run the OS-008 package dry run with:
  `bash field-year-crop/scripts/smoke/production_dry_run.sh`
- The dry run uses tiny synthetic per-year/tile outputs and writes a delivery
  package under `field-year-crop/.run/production-dry-run/delivery`.
- Expected package files are `field_crop_year_counts_all.csv`,
  `field_crop_year_summary_all.csv`, `merge_metadata.json`,
  `delivery_manifest.json`, `gdrive_publish_plan.json`, and
  `delivery_validation.json`.
- `gdrive_publish_plan.json` is a deterministic plan only. Upload remains a
  separate human-gated operator action.

