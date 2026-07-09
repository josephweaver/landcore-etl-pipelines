# OS-007: Real HPCC One-Tile Preflight

Status: Proposed  
Scope: LandCore repository only  
Do not modify: GORC core, GORC worker, GORC controller, GORC data-asset providers, GORC geospatial plugins

Recommended model: 5.4  
Recommended reasoning: high


## Purpose

Prepare and run the first real HPCC one-tile LandCore field-crop-year workflow.

This slice is primarily configuration, runbook, and preflight. It should not expand to full production.

## Allowed Files

```text
orchestration/configs/hpcc-controller.template.json
orchestration/configs/hpcc-worker.template.json
orchestration/configs/hpcc-worker-gdrive.template.json
orchestration/submissions/hpcc-field-crop-year-2010.template.json
orchestration/scripts/smoke/hpcc_preflight.sh
orchestration/scripts/smoke/hpcc_one_tile_2010.sh
orchestration/docs/hpcc-runbook.md
orchestration/docs/runbook.md
orchestration/docs/STATE.md
orchestration/docs/issues.md
```

## Precondition

OS-006 fake HPCC must pass.

If fake HPCC has not passed, do not run real HPCC.

## Required Preflight Checks

`hpcc_preflight.sh` must check and print:

```text
hostname
current user
working directory
available modules or singularity/apptainer version
Slurm sbatch availability
scratch/project path existence
free disk space for cache/data/publish roots
GORC worker executable or Singularity image path
GDAL availability inside the worker image
rclone availability if using Google Drive later
presence check for a worker-local protected reference such as GOET_GDRIVE_TOKEN, without printing the value
ability to read Yan/Roy staged raster path
ability to write publish root
```

If the preflight includes rclone, it must also print whether these worker config
fields are set without printing secret contents:

```text
enable_gdrive_rclone_provider
rclone_executable
rclone_config_path
```

## Runtime Requirements

The HPCC config should use one tile/year only:

```text
year = 2010
yanroy raster = h18v07
```

Do not add production fanout yet.

## Data Handling

Do not commit real data paths if they reveal private storage layout. Use templates with placeholders:

```text
<HPCC_SCRATCH_ROOT>
<LANDCORE_DATA_ROOT>
<GORC_GDAL_IMAGE>
<YANROY_H18V07_PATH>
<PUBLISH_ROOT>
<RCLONE_CONFIG_PATH>
<GDRIVE_REMOTE_NAME>
```

A local ignored config may be created by the user later, but this OS should commit only templates and documentation.

## Secret Handling

If the HPCC one-tile run uses Google Drive or any other credentialed input, the
submission/workflow must use a GORC sensitive protected reference such as:

```json
{
  "name": "gdrive_token",
  "type": "string",
  "sensitive": true,
  "protected_ref": {
    "provider": "worker_env",
    "key": "GOET_GDRIVE_TOKEN"
  }
}
```

The preflight script may verify that `GOET_GDRIVE_TOKEN` exists in the worker
environment, but it must not echo, hash, truncate, or write the value. Plaintext
credentials, rendered rclone config, OAuth tokens, refresh tokens, and
service-account JSON are out of scope for committed templates.

## Validation Command

```bash
bash orchestration/scripts/smoke/hpcc_preflight.sh
bash orchestration/scripts/smoke/hpcc_one_tile_2010.sh
```

The second command may be documented as manual if direct HPCC access is not available to the implementing model.

## Stop Conditions

Stop and record an issue if:

- HPCC lacks compatible Singularity/Apptainer;
- GDAL worker image cannot run;
- input paths are inaccessible from compute nodes;
- required worker-local protected references are unavailable;
- rclone is required but not installed, not configured, or not enabled;
- worker cannot reach controller;
- publication root is not writable;
- raster values exceed current plugin dtype constraints.

## Completion Criteria

- HPCC preflight script exists.
- HPCC one-tile runbook exists.
- Template configs are present.
- One-tile command is documented.
- No private credentials or sensitive paths are committed.
