# OS-006: Fake HPCC Graduation

Status: Proposed  
Scope: LandCore repository only  
Do not modify: GORC core, GORC worker, GORC controller, GORC data-asset providers, GORC geospatial plugins

Recommended model: 5.4  
Recommended reasoning: medium


## Purpose

Run the LandCore field-crop-year workflow through fake HPCC after the local real-input workflow succeeds.

This slice proves that the LandCore workflow can survive the controller/worker separation and Slurm-like execution model before using real HPCC.

## Allowed Files

```text
orchestration/configs/fake-hpcc-controller.json
orchestration/configs/fake-hpcc-worker.gdal.json
orchestration/submissions/fake-hpcc-field-crop-year-2010.submission.json
orchestration/scripts/smoke/fake_hpcc_field_crop_year_2010.sh
orchestration/docs/fake-hpcc-runbook.md
orchestration/docs/runbook.md
orchestration/docs/STATE.md
orchestration/docs/issues.md
```

## Precondition

OS-005 must pass locally.

If OS-005 has not passed, do not implement this slice.

## Runtime Requirements

The fake HPCC run must use the same logical workflow as local real-input execution.

Allowed differences:

```text
controller config
worker config
runtime root
publish root
paths for staged local data
```

Do not fork the product logic.

## Data Strategy

Use one of two options:

### Option A: synthetic fixture

Use the same tiny synthetic fixture from OS-003 to prove fake HPCC mechanics.

### Option B: small real-input run

Use `/tmp/h18v07.hdr` and CDL 2010 only if the fake HPCC environment can see the required local paths and has enough disk.

Prefer Option A first if environment debugging is expected.

## Validation Command

```bash
bash orchestration/scripts/smoke/fake_hpcc_field_crop_year_2010.sh
```

The smoke script must verify:

- controller starts;
- worker job is submitted;
- worker runs under fake HPCC;
- artifacts are promoted;
- published outputs exist;
- no failed work remains;
- logs are available for failed work if any.

## Stop Conditions

Stop and record an issue if:

- fake HPCC cannot see mounted input paths;
- GDAL worker image is unavailable;
- worker config cannot pass `max_asset_bytes`;
- publication path is not visible from worker;
- failure logs are insufficient to debug.

## Completion Criteria

- Fake HPCC smoke passes.
- Runbook records exact commands.
- Runtime-specific path assumptions are documented.
- Same workflow logic is preserved.

