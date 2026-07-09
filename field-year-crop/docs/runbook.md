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
- The local smoke uses the sibling `../go-etl` checkout under WSL to start the controller,
  which then starts workers through its direct-process scheduler and runs `goet-geospatial`
  against the synthetic `/tmp/landcore-field-year-crop/local-synthetic` inputs.
- Controller logs land in the smoke temp directory as `controller.log`; controller-owned
  worker logs land under `controller-runtime/logs/worker.log`, with attempt logs under
  `controller-runtime/tmp/attempts/<attempt-id>/logs/`.

## Fake HPCC

- Reuse the same project and workflow artifacts.
- Run through the fake HPCC workflow wrapper and capture controller logs.
- Confirm output layout matches the product spec files in `field-year-crop/docs`.

## Real HPCC

- Start with one-tile preflight.
- Confirm publication and manifest paths align with the Google Drive target configured in
  `land-core.project.json`.
- Keep output credentials out of this repository; use runtime environment injection.

