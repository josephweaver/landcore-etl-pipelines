# OS-009: Google Drive Source and Publication Connector Trial

Status: Proposed  
Scope: LandCore repository only  
Do not modify: GORC core, GORC worker, GORC controller, GORC data-asset providers, GORC geospatial plugins

Recommended model: 5.4  
Recommended reasoning: high

## Purpose

After the local, fake-HPCC, and one-tile HPCC paths are understood, try the real
LandCore Google Drive / Shared Drive path for:

```text
input: Yan/Roy release source file
output: tile-field-year-crop publication folder
```

The input side should use GORC's existing `gdrive_rclone` provider when
possible. The output side should prove an explicit rclone publication command or
record that outbound Google Drive publication remains manual or blocked.

This is a late slice because it depends on external runtime setup: rclone,
worker-visible credentials, remote naming, Shared Drive access, and large-file
transfer behavior. It should not block the earlier local-file or
registered-location product path.

## Current State

GORC already has:

```text
gdrive_rclone data provider
rclone_executable worker config
rclone_config_path worker config
enable_gdrive_rclone_provider worker config
worker_env protected references
Python subprocess secret materialization
controlled stdout/stderr/status redaction for exact materialized secrets
```

The LandCore field-crop-year SC has not yet proven that a real worker can read a
LandCore Google Drive or Shared Drive source with those capabilities.

## Target State

A manual smoke path exists that proves one small or explicitly selected
Google Drive source can be acquired into the worker asset cache through
`gdrive_rclone`, without committing credentials or private rendered rclone
configuration. A second manual smoke path exists that proves a tiny delivery
file can be copied to the intended Google Drive folder, or records the exact
blocker.

If the real connector cannot be made to work inside this slice, the blocker is
recorded and production remains allowed to use `local_file` or
`registered_location` Yan/Roy inputs and manual publication.

## Allowed Files

```text
orchestration/projects/landcore-field-crop-year.project.json
orchestration/workflows/gdrive-yanroy-connector-trial.workflow.json
orchestration/submissions/gdrive-yanroy-connector-trial.template.json
orchestration/configs/local-worker-gdrive.template.json
orchestration/configs/hpcc-worker-gdrive.template.json
orchestration/scripts/python/inspect_materialized_asset.py
orchestration/scripts/python/check_secret_materialization.py
orchestration/scripts/python/write_gdrive_publish_probe.py
orchestration/scripts/smoke/gdrive_connector_preflight.sh
orchestration/scripts/smoke/gdrive_connector_trial.sh
orchestration/scripts/smoke/gdrive_publish_probe.sh
orchestration/docs/gdrive-connector-runbook.md
orchestration/docs/runbook.md
orchestration/docs/STATE.md
orchestration/docs/issues.md
```

## Source Connector Shape

The workflow should declare a `gdrive_rclone` data asset for the Yan/Roy release
source file or a smaller operator-approved test object.

The production Yan/Roy source is:

```text
https://drive.google.com/file/d/1YmFECConwSlAFEaMDzyL_srhwVfeTRBy/view?usp=drive_link
file ID: 1YmFECConwSlAFEaMDzyL_srhwVfeTRBy
```

```json
{
  "binding_name": "yanroy_release_drive",
  "provider_name": "yanroy_release_drive",
  "kind": "field_boundary_archive",
  "format": "seven_zip",
  "provider": "gdrive_rclone",
  "location": {
    "type": "gdrive_rclone",
    "remote": "<GDRIVE_REMOTE_NAME>",
    "path": "<SAFE_DRIVE_RELATIVE_PATH>"
  },
  "cache": {
    "strategy": "worker_cache",
    "cache_key": "gdrive/landcore/yanroy/release-data/source.7z",
    "immutable": true
  },
  "materialization": {
    "strategy": "worker_cache"
  }
}
```

Prefer file-ID based access if current GORC/rclone support can safely express
it. If current GORC `gdrive_rclone` only supports path-based access, record that
file-ID support is blocked and use a placeholder path in committed templates
until the operator supplies the rclone-visible path.

Use placeholders in committed templates for remote names and any private
Drive-relative paths. The public file ID above may be committed.

## Publication Connector Shape

The intended durable destination for the finished tile-field-year-crop product is:

```text
https://drive.google.com/drive/folders/1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4?usp=drive_link
folder ID: 1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4
```

If GORC does not support outbound `commit_data` to `gdrive_rclone`, do not patch
GORC in this LandCore slice. Instead, write a LandCore-side publication probe
that uses configured rclone directly against a tiny non-private fixture file.

The probe should prove:

```text
rclone can address the folder ID or an operator-supplied remote path for that folder
a tiny fixture file can be copied to the folder
the copied object can be listed or checked
the local probe records uploaded object evidence
the probe does not print or persist credential values
```

Do not run a full delivery upload automatically.

## Secret Propagation Requirement

If a credential value must be passed into a work item, it must use a sensitive
`worker_env` protected reference:

```json
{
  "name": "gdrive_token",
  "type": "string",
  "sensitive": true,
  "protected_ref": {
    "provider": "worker_env",
    "key": "GOET_GDRIVE_TOKEN"
  },
  "redaction_label": "${worker_env.GOET_GDRIVE_TOKEN}",
  "materialize": {
    "mode": "env",
    "target": "GDRIVE_TOKEN"
  }
}
```

Prefer a preconfigured worker rclone remote when possible. In that pattern, the
workflow names only the remote plus file/folder ID or path, while credentials
live in the worker environment or mounted rclone config. The committed worker
template may name:

```json
{
  "enable_gdrive_rclone_provider": true,
  "rclone_executable": "<RCLONE_EXECUTABLE>",
  "rclone_config_path": "<RCLONE_CONFIG_PATH>"
}
```

Do not commit:

```text
OAuth access tokens
OAuth refresh tokens
service-account JSON
rendered rclone config
raw GOET_GDRIVE_TOKEN values
private local absolute paths
```

## Preflight

`gdrive_connector_preflight.sh` must check:

```text
rclone executable exists
rclone version prints
worker config enables gdrive_rclone
worker config has rclone_executable
worker config has rclone_config_path or documented env-based remote setup
GOET_GDRIVE_TOKEN exists if the trial uses protected-reference materialization
the script does not print GOET_GDRIVE_TOKEN
the configured rclone remote can list or stat the selected source file ID or path
the configured rclone remote can list or write to the selected publication folder ID or path when the publish probe is enabled
free cache space is sufficient for the selected source
```

The preflight may print whether a secret variable is present, but it must not
print, hash, truncate, encode, or write the secret value.

## Trial Workflow

The trial workflow should:

1. materialize the selected `gdrive_rclone` data asset;
2. inspect `GOET_DATA_ASSETS_JSON` with `inspect_materialized_asset.py`;
3. write compact evidence with local path, provider, cache key, byte count,
   SHA-256, and archive metadata when available;
4. optionally run `check_secret_materialization.py` only to prove protected
   reference materialization and redaction behavior with a harmless sentinel
   secret, not a real Google credential;
5. avoid extracting the full Yan/Roy archive unless the operator explicitly
   approves the runtime cost.

## Publication Probe

`gdrive_publish_probe.sh` should:

1. create or use one tiny non-private CSV fixture;
2. copy it to Google Drive folder `1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4` through
   the configured rclone remote;
3. list or check the uploaded object;
4. write compact local evidence with source path, target folder ID, target object
   name, byte count, SHA-256, and timestamp;
5. optionally delete the probe object only if the operator explicitly enables
   cleanup.

The probe must not upload private raster inputs or full production outputs.

## Validation Command

```bash
bash orchestration/scripts/smoke/gdrive_connector_preflight.sh
bash orchestration/scripts/smoke/gdrive_connector_trial.sh
bash orchestration/scripts/smoke/gdrive_publish_probe.sh
```

The second and third commands may be documented as manual when the implementing
model does not have access to the real Google Drive remote or key.

## Stop Conditions

Stop and record an issue if:

- rclone is unavailable in the worker runtime;
- `enable_gdrive_rclone_provider` is false or missing;
- `rclone_executable` or `rclone_config_path` cannot be configured without
  committing private details;
- the worker cannot resolve the required `worker_env` protected reference;
- the rclone remote cannot read the selected source;
- current GORC `gdrive_rclone` cannot acquire the source by file ID and no
  operator-approved path is available;
- the rclone remote cannot write a tiny probe object to folder
  `1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4`;
- the selected source exceeds `max_asset_bytes` or available cache space;
- rclone output or GORC status/log surfaces contain a raw secret value;
- the real Yan/Roy archive requires `seven_zip` extraction and no approved
  extractor is configured.

## Out Of Scope

- Implementing a native Google Drive API provider.
- Modifying GORC `gdrive_rclone` provider code.
- Uploading full production output automatically.
- Syncing entire folders.
- Running full production fanout.
- Committing private credentials, rendered config, private paths, or large data.
- Registering outputs in the LandCore data catalog.

## Completion Criteria

- Google Drive connector preflight exists.
- Worker templates document the required rclone and protected-reference setup.
- The trial workflow can acquire a selected Google Drive source, or a blocker is
  recorded with the exact failed check.
- The publication probe can upload a tiny non-private object to Google Drive
  folder `1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4`, or a blocker is recorded with the
  exact failed check.
- Credential handling uses worker-local protected references when a work-item
  secret is needed.
- Controlled logs/status are checked for the sentinel secret when the
  materialization smoke is run.
- The runbook states whether production should use `gdrive_rclone`,
  `local_file`, or `registered_location` for Yan/Roy release acquisition and
  whether Google Drive publication is automated, manual, or blocked.
