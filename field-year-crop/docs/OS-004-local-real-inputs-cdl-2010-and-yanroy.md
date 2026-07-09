# OS-004: Local Real Inputs â€” CDL 2010 and Yan/Roy h18v07

Status: Proposed
Scope: LandCore repository only
Do not modify: GORC core, GORC worker, GORC controller, GORC data-asset providers, GORC geospatial plugins

Recommended model: 5.4-mini
Recommended reasoning: high


## Purpose

Extend the local workflow from synthetic fixtures to real input materialization:

```text
Yan/Roy raster: /tmp/h18v07.hdr
CDL ZIP: https://www.nass.usda.gov/Research_and_Science/Cropland/Release/datasets/2010_30m_cdls.zip
```

This slice proves input visibility and metadata. It does not need to produce final field-crop-year output yet.

This slice may use a manually staged local Yan/Roy file or a configured
registered location. Do not make real Google Drive access a requirement here;
OS-009 owns the credentialed `gdrive_rclone` connector trial.

## Allowed Files

```text
field-year-crop/workflows/local-real-input-metadata-2010.workflow.json
field-year-crop/submissions/local-real-input-metadata-2010.submission.json
field-year-crop/configs/local-worker.gdal.json
field-year-crop/scripts/python/discover_raster_asset.py
field-year-crop/scripts/python/run_raster_info.py
field-year-crop/scripts/smoke/local_real_input_metadata_2010.sh
field-year-crop/docs/runbook.md
field-year-crop/docs/STATE.md
field-year-crop/docs/issues.md
```

## Required Input Assumptions

The local machine has:

```text
/tmp/h18v07.hdr
```

Important: this may not be enough. If it is an ENVI header, GDAL may require paired binary data files next to it.

The CDL URL is:

```text
https://www.nass.usda.gov/Research_and_Science/Cropland/Release/datasets/2010_30m_cdls.zip
```

If the local Yan/Roy input is not `/tmp/h18v07.hdr`, the replacement must still
be explicit and non-secret, such as:

```text
registered_location: <configured root> + <safe relative tile path>
local_file: <configured root> + <safe relative tile path>
```

Do not commit private absolute paths.

## Worker Config Requirement

The worker config must explicitly raise `max_asset_bytes`.

Suggested first value:

```json
"max_asset_bytes": 20000000000
```

Reason: real CDL ZIPs are much larger than fixture-sized defaults.

## CDL Archive Strategy

Do not hard-code an internal archive member name at first.

Preferred approach:

```text
HTTP data asset -> worker cache -> ZIP selected_directory extraction -> Python discovers the actual raster
```

If current GORC archive extraction requires explicit selected members and cannot extract the CDL directory generically, record a blocker in `field-year-crop/docs/issues.md` and implement a LandCore-side prefetch script as a temporary local pilot workaround.

## Credential Rule

No plaintext credentials are allowed in this slice. If a future local run needs
a credentialed input, declare a sensitive `worker_env` protected reference and
stop unless the worker runtime can resolve it without exposing the value to the
controller:

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

The actual Google Drive/rclone proof remains OS-009.

## Metadata Step

Create a Python script that:

1. discovers the primary CDL raster inside the extracted CDL directory;
2. validates that `/tmp/h18v07.hdr` exists inside the worker-visible path;
3. calls `goet-geospatial` `raster_info` for both inputs;
4. writes metadata artifacts:
   - `metadata/cdl_2010_raster_info.json`
   - `metadata/yanroy_h18v07_raster_info.json`
   - `metadata/input_discovery.json`

## Required Checks

The metadata output must answer:

```text
Can GDAL open /tmp/h18v07.hdr?
What sidecars are needed, if any?
What is the CDL primary raster member path?
What are width/height/band count?
What are CRS/bounds/geotransform?
What nodata value is present?
Does Yan/Roy field_id appear to fit uint16?
```

If field IDs cannot be range-checked from metadata alone, document that the range check moves to OS-005.

## Validation

```bash
bash field-year-crop/scripts/smoke/local_real_input_metadata_2010.sh
```

The smoke should fail clearly if:

- CDL download fails;
- CDL archive cannot be extracted;
- no CDL raster is discoverable;
- multiple CDL rasters are discovered and no selector is provided;
- `/tmp/h18v07.hdr` cannot be opened by GDAL;
- metadata JSON files are missing.

## Stop Conditions

Stop and record an issue if:

- `h18v07.hdr` requires missing sidecars;
- CDL archive extraction cannot be represented without changing GORC;
- `max_asset_bytes` is not honored;
- GDAL cannot open one of the inputs;
- the two raster bounds clearly do not overlap.

## Completion Criteria

- Real CDL 2010 input is materialized or prefetched.
- Real Yan/Roy `h18v07` input is visible to the worker.
- Raster metadata artifacts are produced.
- The runbook documents exact local setup.
- No pair counts are required yet.
