# LandCore Field-Crop-Year Data Product Planning Bundle

This bundle contains a LandCore-side Strategic Concept (SC) and concrete Operational Slices (OS) for building a field-crop-year data product using GORC as an external runtime.

The intended implementation location is the existing LandCore repository, under a folder such as:

```text
orchestration/
```

Project facts that should be carried into the LandCore project document:

```text
LandCore workflow repository:
  https://github.com/josephweaver/landcore-etl-pipelines.git

LandCore data catalog repository:
  https://github.com/land-core/landcore-data-catalog.git
```

Google Drive product endpoints:

```text
Yan/Roy release source file:
  https://drive.google.com/file/d/1YmFECConwSlAFEaMDzyL_srhwVfeTRBy/view?usp=drive_link
  file ID: 1YmFECConwSlAFEaMDzyL_srhwVfeTRBy

Tile-field-year-crop publication folder:
  https://drive.google.com/drive/folders/1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4?usp=drive_link
  folder ID: 1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4
```

These Drive IDs are product endpoint identifiers, not secrets. Real access is
expected to fail until the operator loads the required key or rclone credential
into the worker runtime.

This bundle intentionally does **not** define changes to GORC core or GORC geospatial plugins. The implementation should use:

- `project.json`
- `workflow.json`
- submission JSON
- controller/worker config JSON
- Python scripts
- R scripts only if needed later
- runbooks and smoke scripts

Primary product goal:

```text
Yan/Roy field-id raster + USDA CDL crop raster
  -> aligned categorical rasters
  -> field_id,crop_id,year,pixel_count
  -> field/year/crop summary table
```

User-provided starting inputs:

```text
Yan/Roy raster header staged locally:
  /tmp/h18v07.hdr

USDA CDL 2010 ZIP:
  https://www.nass.usda.gov/Research_and_Science/Cropland/Release/datasets/2010_30m_cdls.zip
```

Important caveat: `/tmp/h18v07.hdr` may require one or more sidecar data files. The first real-input slice must prove that `gdalinfo /tmp/h18v07.hdr` works inside the selected GDAL/GORC worker environment before attempting the data product.

Credentialed inputs should use GORC phase-1 sensitive-variable propagation:
workflow documents may declare `worker_env` protected references such as
`GOET_GDRIVE_TOKEN`, but must not contain plaintext Google Drive credentials or
rendered rclone config content. The late Google Drive slice exists to prove the
real `gdrive_rclone` source path and finished-output publication path after
local and HPCC basics are understood.

Production scope is limited to LandCore's states of interest:

```text
IL, MN, WI, OH, SD, IA, IN, MI, MO
```

Production tile templates must use the tile list in
`SC-landcore-field-crop-year-data-product.md`; pilots may use smaller explicit
subsets such as `h18v07`.
