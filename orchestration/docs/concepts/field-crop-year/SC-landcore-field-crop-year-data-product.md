# SC: LandCore Field-Crop-Year Data Product

Status: Draft  
Scope: LandCore repository only  
Cadence: CSxIx, grouped Operational Slice planning followed by implementation  
Runtime dependency: GORC / go-etl, pinned by commit in `orchestration/GORC_VERSION.md`  
Last updated: 2026-07-08

## Purpose

Build a LandCore-side data product that computes field-by-crop composition by year from:

```text
Yan/Roy field-id raster + USDA CDL crop-code raster
```

The first target year is 2010. The first local Yan/Roy raster is expected to be staged at:

```text
/tmp/h18v07.hdr
```

The first CDL source is:

```text
https://www.nass.usda.gov/Research_and_Science/Cropland/Release/datasets/2010_30m_cdls.zip
```

The intended production Yan/Roy source is the Google Drive file:

```text
https://drive.google.com/file/d/1YmFECConwSlAFEaMDzyL_srhwVfeTRBy/view?usp=drive_link
Google Drive file ID: 1YmFECConwSlAFEaMDzyL_srhwVfeTRBy
```

The intended production publication target for tile-field-year-crop outputs is:

```text
https://drive.google.com/drive/folders/1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4?usp=drive_link
Google Drive folder ID: 1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4
```

Access to both Google Drive locations is expected to fail until the operator
loads the required key or rclone credential into the worker runtime.

This is a **LandCore workflow/product SC**, not a GORC core SC. It should not modify GORC controller, worker, geospatial plugin, or data-asset provider code.

## LandCore Project Scope

The LandCore project document created by this SC must identify the workflow
source repository and the data catalog repository as explicit project facts:

```text
LandCore workflow repository: https://github.com/josephweaver/landcore-etl-pipelines.git
LandCore data catalog repository: https://github.com/land-core/landcore-data-catalog.git
```

The LandCore project document must also record these Google Drive product
endpoints as non-secret IDs:

```yaml
yanroy_release_drive_file_id: 1YmFECConwSlAFEaMDzyL_srhwVfeTRBy
tile_field_year_crop_publish_drive_folder_id: 1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4
```

The first production scope is limited to these states:

```yaml
states_of_interest: ['IL', 'MN', 'WI', 'OH', 'SD', 'IA', 'IN', 'MI', 'MO']
```

The first production scope is limited to these Yan/Roy tiles:

```yaml
tiles_of_interest:
  - h12v04
  - h12v05
  - h12v06
  - h12v07
  - h13v04
  - h13v05
  - h13v06
  - h13v07
  - h14v04
  - h14v05
  - h14v06
  - h14v07
  - h15v05
  - h15v06
  - h15v07
  - h16v02
  - h16v03
  - h16v04
  - h16v05
  - h16v06
  - h16v07
  - h16v08
  - h17v02
  - h17v03
  - h17v04
  - h17v05
  - h17v06
  - h17v07
  - h17v08
  - h17v09
  - h17v10
  - h17v11
  - h18v04
  - h18v05
  - h18v06
  - h18v07
  - h18v08
  - h18v09
  - h18v10
  - h18v11
  - h18v12
  - h19v04
  - h19v05
  - h19v06
  - h19v07
  - h19v08
  - h19v09
  - h19v10
  - h19v12
  - h20v05
  - h20v06
  - h20v07
  - h20v08
  - h20v09
  - h20v10
  - h20v11
  - h20v12
  - h21v04
  - h21v05
  - h21v06
  - h21v07
  - h21v08
  - h21v09
  - h21v10
  - h21v11
  - h22v04
  - h22v05
  - h22v06
  - h22v07
  - h22v08
  - h22v09
  - h22v10
  - h23v04
  - h23v05
  - h23v06
  - h23v07
  - h23v08
  - h23v09
  - h23v10
  - h24v06
  - h24v07
  - h24v08
  - h24v09
  - h24v10
  - h25v07
  - h25v08
  - h25v09
```

Implementation may use a smaller pilot subset, but production templates and
delivery manifests must not silently expand beyond these states and tiles.

## Product Definition

### Primary output

`field_crop_year_counts.csv`

One row per observed `(field_id, crop_id, year)` pair:

```csv
field_id,crop_id,year,pixel_count
101,1,2010,817
101,5,2010,29
102,24,2010,1402
```

### Secondary output

`field_crop_year_summary.csv`

One row per observed `(field_id, crop_id, year)` pair with field totals and crop shares:

```csv
field_id,year,crop_id,pixel_count,total_field_pixels,share,is_dominant,dominant_crop_id,dominant_crop_share
101,2010,1,817,846,0.965721,true,1,0.965721
101,2010,5,29,846,0.034279,false,1,0.965721
```

Optional later columns:

```text
crop_name
pixel_area_m2
acres
source_cdl_year
yanroy_raster_id
cdl_source_uri
field_raster_path
aligned_cdl_metadata_sha256
```

## Ownership Boundary

GORC owns orchestration behavior.

LandCore workflow assets own the LandCore data product.

Implementation should be confined to the LandCore repository, preferably under:

```text
orchestration/
  README.md
  GORC_VERSION.md
  projects/
  workflows/
  submissions/
  configs/
  docs/
  scripts/
  tests/
```

Allowed implementation assets:

```text
orchestration/projects/*.json
orchestration/workflows/*.json
orchestration/submissions/*.json
orchestration/configs/*.json
orchestration/scripts/**/*.py
orchestration/scripts/**/*.R
orchestration/scripts/**/*.sh
orchestration/docs/**/*.md
orchestration/tests/**
```

Do not modify:

```text
../go-etl/**
GORC controller code
GORC worker code
GORC geospatial plugin code
GORC data-asset provider code
```

If GORC cannot support a needed behavior, stop and record the blocker in:

```text
orchestration/docs/issues.md
```

Do not silently patch GORC during this LandCore implementation pass.

## Credential Boundary

GORC now has phase-1 sensitive-variable propagation for worker-local protected
references and controlled-output redaction. LandCore credentialed workflow
configuration should use that boundary when the workflow needs a secret.

For this SC, the approved credential pattern is:

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

Rules:

- workflow and project JSON may name protected references, redaction labels, and
  rclone provider configuration, but must not contain OAuth tokens, refresh
  tokens, service-account JSON, or rendered rclone config contents;
- the controller must see protected references, not plaintext credentials;
- workers may resolve `worker_env` protected references only at execution time;
- Python scripts must receive secrets only through explicit materialization
  surfaces such as an env var or protected temp file;
- GOET phase 1 redacts exact materialized values from controlled logs and
  status, but LandCore scripts must still avoid writing secrets to artifacts,
  network calls, screenshots, encoded strings, or external logs.

Credentialed Google Drive work should use the existing GORC `gdrive_rclone`
provider where possible. Rclone executable path, rclone config path, configured
remote, and credential provisioning are deployment/runtime concerns, not
workflow-source contents.

The Google Drive file ID and folder ID above are not credentials. They may be
committed as product endpoint identifiers. The key, token, service-account JSON,
OAuth refresh token, rendered rclone config, or other access material must not
be committed.

## Runtime Strategy

Implementation progresses through six gates:

```text
Gate 1: Local synthetic fixture
Gate 2: Local real-input pilot: /tmp/h18v07.hdr + 2010 CDL ZIP
Gate 3: Fake-HPCC execution
Gate 4: Real HPCC one-tile run
Gate 5: Production tiling/year expansion
Gate 6: Optional Google Drive source and publication connector trial
```

The first working product should be small, inspectable, and reproducible. Do not start with a full production run.

## Data State Transitions

The product should be reasoned about as data-state transitions, not just code steps.

```text
Raw input references
  -> cache_data(cdl year)
  -> cache_data(Yan/Roy tile)
  -> raster_info(Yan/Roy tile and CDL year)
  -> align_to_grid(CDL year, like_raster=Yan/Roy tile)
  -> raster_pair_value_counts(Yan/Roy tile, aligned CDL year tile)
  -> field_id,crop_id,year,tile summary table
  -> published local outputs and run evidence
  -> Google Drive delivery folder for tile-field-year-crop outputs
```

## Geospatial Plugin Reuse Decision

LandCore scripts should orchestrate existing GORC capabilities instead of
reimplementing raster processing. The production shape is:

```text
cache_data(cdl_year)
cache_data(yanroy_tile)
raster_info(yanroy_tile)
raster_info(cdl_year)
align_to_grid(source_raster=cdl_year, like_raster=yanroy_tile, resampling=nearest)
raster_pair_value_counts(field_raster=yanroy_tile, value_raster=aligned_cdl_tile)
summarize_field_crop_counts
validate_field_crop_year_product
publish delivery package
```

Fanout policy:

```text
cache_data(cdl_year): one per year, deduplicated by cache key
cache_data(yanroy_tile): one per tile in tiles_of_interest
raster_info(yanroy_tile): one per tile
align_to_grid: one per year x tile
raster_pair_value_counts: one per year x tile
summarize/validate: one per year x tile, followed by merge
```

`raster_info` is the "get extents" operation. It records the Yan/Roy tile
bounds, dimensions, CRS, transform, band metadata, and nodata evidence. The core
workflow should not implement a separate LandCore extent reader.

`align_to_grid` is the intended crop/reproject/resample step for CDL. When it is
called with `like_raster=Yan/Roy tile`, the existing geospatial plugin uses the
Yan/Roy tile grid as the target grid. That produces a CDL raster on the same
extent, dimensions, CRS, transform, and pixel grid as the Yan/Roy tile.

Do not add a separate `crop(cdl.path, yanroy extents)` script for the core
field-crop-year path. Use `align_to_grid` first. Use `crop_by_polygons` only if
a later slice introduces vector polygon AOIs or a fixture that explicitly needs
polygon bounding-box/cutline crops. Current production tiling is raster-tile
driven by Yan/Roy tile rasters, not polygon-driven.

`raster_pair_value_counts` is the field/crop counting operation. It should run
only after `align_to_grid` has produced a CDL raster aligned to the Yan/Roy
tile. It must use `require_aligned_grid=true`.

### Transition 1: Materialize inputs

Inputs:

```text
CDL 2010 ZIP over HTTP
Yan/Roy local raster path /tmp/h18v07.hdr, registered location, or gdrive_rclone-acquired Google Drive file 1YmFECConwSlAFEaMDzyL_srhwVfeTRBy
```

Outputs:

```text
worker-local CDL archive/extracted directory
worker-local Yan/Roy raster reference or copy
```

Invariant:

```text
Both inputs are visible inside the worker environment.
gdalinfo succeeds on the actual raster paths.
```

### Transition 2: Inspect raster metadata with `raster_info`

Inputs:

```text
worker-local Yan/Roy raster
worker-local CDL raster discovered from extracted CDL ZIP
```

Outputs:

```text
metadata/yanroy_raster_info.json
metadata/cdl_raster_info.json
metadata/input_bounds.json
```

Invariant:

```text
The rasters have valid dimensions, band 1, declared or detected nodata, and overlapping spatial bounds after CRS transformation.
```

### Transition 3: Align CDL to Yan/Roy grid

Inputs:

```text
Yan/Roy field-id raster
CDL crop-code raster
```

Outputs:

```text
aligned/cdl_2010_on_h18v07_grid.tif
aligned/alignment_metadata.json
```

Invariant:

```text
Aligned CDL has the same width, height, CRS, geotransform, and pixel grid as the Yan/Roy raster.
Nearest-neighbor resampling is used for categorical CDL values.
No separate LandCore crop-by-extent script is used for this core path.
```

### Transition 4: Pair count with `raster_pair_value_counts`

Inputs:

```text
Yan/Roy field-id raster
Aligned CDL raster
```

Outputs:

```text
counts/field_crop_counts_2010.csv
counts/field_crop_counts_2010.metadata.json
```

Invariant:

```text
Rows are deterministic and sorted by field_id, crop_id.
0 is treated as nodata unless metadata proves otherwise.
field_id and crop_id values fit the current geospatial operation dtype constraints.
```

### Transition 5: Summarize

Inputs:

```text
counts/field_crop_counts_2010.csv
year = 2010
```

Outputs:

```text
summary/field_crop_year_summary_2010.csv
summary/field_crop_year_summary_2010.metadata.json
```

Invariant:

```text
For each field_id/year, shares sum to approximately 1.0 across crop rows.
Dominant crop is deterministic under ties.
No negative counts.
No missing year.
```

### Transition 6: Publish

Inputs:

```text
promoted compute artifacts
```

Outputs:

```text
published field-crop-year CSVs
published metadata JSON
published run manifest
Google Drive delivery folder contents when credentialed publication is enabled
```

Invariant:

```text
Published files match promoted artifact hashes and byte counts.
Publication path is safe, relative, and deterministic.
Google Drive delivery uses folder ID 1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4 and records uploaded object evidence.
```

## Known Gotchas

### Yan/Roy `.hdr` sidecars

`/tmp/h18v07.hdr` may be only a header. The real raster may require a paired binary file or sidecar files. First real-input validation must run `gdalinfo /tmp/h18v07.hdr` inside the worker environment.

If GDAL cannot open it, stop and record which sidecars are missing.

### CDL ZIP member names are not assumed

Do not hard-code the internal CDL ZIP member name until the implementation inspects the actual archive.

Preferred first approach:

```text
materialize/extract CDL ZIP as a directory
Python discovers the primary raster member by extension and GDAL readability
```

Candidate raster extensions:

```text
.tif
.tiff
.img
.bil
.hdr
```

If multiple candidates exist, require an explicit selector.

### GORC worker default max asset size may be too small

The GORC worker default max asset size is fixture-sized. Real CDL ZIPs are large. LandCore worker configs must explicitly set a large `max_asset_bytes` value or the CDL download/materialization will fail before Python starts.

Suggested local pilot setting:

```json
"max_asset_bytes": 20000000000
```

Adjust downward only after measuring actual input size.

### Credentialed Google Drive is available but not automatic

GORC has an implemented `gdrive_rclone` data provider and worker-local
protected-reference secret propagation. That does not mean a real LandCore
Shared Drive run will work without runtime setup.

Before using Google Drive for Yan/Roy input acquisition or finished-product
publication, a LandCore slice must
prove:

```text
rclone is installed in the worker image or environment
enable_gdrive_rclone_provider is true
rclone_executable is configured
rclone_config_path or equivalent worker environment setup is present
the rclone remote can read the target LandCore Shared Drive path
the rclone remote can write to the tile-field-year-crop delivery folder when publication is enabled
GOET_GDRIVE_TOKEN or another approved worker-local secret is available only to the worker
controlled logs/status do not contain the raw secret
```

If any of those checks fail, fall back to a manually staged `local_file` or
`registered_location` Yan/Roy input and record the Google Drive blocker in
`orchestration/docs/issues.md`.

### Current geospatial pair counting assumes uint16 categories

The current reusable geospatial path is designed around `uint16` field and crop IDs. CDL crop IDs should fit. Yan/Roy field IDs might not.

Before running pair counts at scale, inspect the Yan/Roy field raster value range. If field IDs exceed 65535, stop and record a GORC/plugin blocker. Do not coerce field IDs modulo 65536.

### No first-class `geospatial_operation` work item yet

For this LandCore implementation, invoke the reusable `goet-geospatial` executable from a `python_script` work item.

Do not modify GORC core to add a new work-item type during this SC.

### Local full CDL may be heavy

A national CDL raster is large. The local real-input pilot should align CDL to the Yan/Roy tile grid and avoid producing unnecessary full-CONUS intermediate outputs.

If local disk or runtime becomes an issue, stop and graduate earlier to fake HPCC/HPCC with a smaller crop window or tile-specific alignment.

### Raster overlap may be zero

The Yan/Roy tile `h18v07` may not overlap the target CDL raster extent or may use a CRS requiring reprojection. The local real-input slice must inspect bounds before pair counting.

## Implementation Order

1. Repo layout and product contract.
2. Python script contracts with synthetic unit fixtures.
3. Local synthetic GORC workflow.
4. Local real-input materialization for `/tmp/h18v07.hdr` and CDL 2010 ZIP.
5. Local real-input pair count and summary.
6. Fake HPCC run.
7. Real HPCC one-tile preflight/run.
8. Production tiling, delivery, and provenance package.
9. Late Google Drive source and publication connector trial for the Yan/Roy
   release file and tile-field-year-crop publication folder.

## Non-Goals

This SC does not implement:

- changes to GORC controller code;
- changes to GORC worker code;
- changes to GORC geospatial plugin code;
- new GORC work-item types;
- full RCI modeling;
- private LandCore methodology;
- multi-year production expansion before the 2010 one-tile pilot passes;
- production outside the LandCore states and tiles listed in this SC unless a
  later SC revision changes that scope;
- storage of large rasters in Git;
- publication of private or purchased data in public repositories.

## Completion Definition

The SC is complete when the LandCore repository can run a documented workflow that:

1. materializes or references the 2010 CDL raster and `/tmp/h18v07.hdr`;
2. verifies both rasters with GDAL metadata;
3. aligns CDL to the Yan/Roy grid;
4. emits `field_id,crop_id,year,pixel_count`;
5. emits a deterministic summary CSV;
6. publishes outputs to a named local location;
7. reproduces locally and in fake HPCC;
8. has a real HPCC one-tile runbook;
9. records the LandCore workflow repository, LandCore data catalog repository,
   states of interest, and tiles of interest in project or production docs;
10. either proves the real Google Drive/rclone credential path or records why
    production should use `local_file` or `registered_location` instead;
11. records whether tile-field-year-crop output publication to Google Drive
    folder `1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4` is automated, manual, or blocked;
12. records unresolved production blockers explicitly.
