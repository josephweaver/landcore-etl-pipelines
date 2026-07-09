# Field-Crop-Year Runbook

This runbook captures the staged execution path:

1. local debug orchestration
2. fake HPCC run
3. real HPCC preflight and scale-out

## Local Debug

- Validate `land-core.project.json` is present.
- Ensure `field-year-crop/docs/data-product-spec.md` matches the expected output schema.
- Run the local synthetic/fixture workflow before first real input work.

## Fake HPCC

- Reuse the same project and workflow artifacts.
- Run through the fake HPCC workflow wrapper and capture controller logs.
- Confirm output layout matches the product spec files in `field-year-crop/docs`.

## Real HPCC

- Start with one-tile preflight.
- Confirm publication and manifest paths align with the Google Drive target configured in
  `land-core.project.json`.
- Keep output credentials out of this repository; use runtime environment injection.

