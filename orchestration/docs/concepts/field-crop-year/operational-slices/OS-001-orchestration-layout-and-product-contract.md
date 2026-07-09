# OS-001: Orchestration Layout and Product Contract

Status: Proposed  
Scope: LandCore repository only  
Do not modify: GORC core, GORC worker, GORC controller, GORC data-asset providers, GORC geospatial plugins

Recommended model: 5.3-Codex-Spark  
Recommended reasoning: low


## Purpose

Create the LandCore-side orchestration layout and write the initial product contract for the field-crop-year data product.

This slice does not run GORC. It creates the file structure that later slices will fill in.

## Allowed Files

```text
orchestration/README.md
orchestration/GORC_VERSION.md
orchestration/docs/STATE.md
orchestration/docs/issues.md
orchestration/docs/data-product-spec.md
orchestration/docs/runbook.md
orchestration/projects/landcore-field-crop-year.project.json
orchestration/workflows/.gitkeep
orchestration/submissions/.gitkeep
orchestration/configs/.gitkeep
orchestration/scripts/python/.gitkeep
orchestration/scripts/smoke/.gitkeep
orchestration/tests/fixtures/.gitkeep
```

## Required Content

### `orchestration/README.md`

Must state:

```text
This folder contains LandCore-specific workflows, scripts, configuration, and documentation for producing LandCore data products using GORC.

GORC is a separate reusable orchestration runtime maintained outside this repository.
```

Must include:

```text
local debug -> fake HPCC -> real HPCC
```

### `orchestration/GORC_VERSION.md`

Must include placeholders:

```md
# GORC Runtime Version

- Repository: https://github.com/josephweaver/go-etl
- Commit: <fill before run>
- Date tested: <fill before run>
- Runtime mode: local
- Notes:
```

### `orchestration/projects/landcore-field-crop-year.project.json`

Must include:

```json
{
  "id": "landcore-field-crop-year",
  "name": "LandCore Field-Crop-Year Data Product",
  "source_repositories": {
    "landcore_etl_pipelines": "https://github.com/josephweaver/landcore-etl-pipelines.git",
    "landcore_data_catalog": "https://github.com/land-core/landcore-data-catalog.git"
  },
  "google_drive_endpoints": {
    "yanroy_release_file_id": "1YmFECConwSlAFEaMDzyL_srhwVfeTRBy",
    "yanroy_release_url": "https://drive.google.com/file/d/1YmFECConwSlAFEaMDzyL_srhwVfeTRBy/view?usp=drive_link",
    "tile_field_year_crop_publish_folder_id": "1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4",
    "tile_field_year_crop_publish_url": "https://drive.google.com/drive/folders/1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4?usp=drive_link"
  },
  "states_of_interest": ["IL", "MN", "WI", "OH", "SD", "IA", "IN", "MI", "MO"],
  "tiles_of_interest": ["h18v07"]
}
```

`tiles_of_interest` may start with `h18v07` for the pilot, but the file or a
production companion document must also point to the complete production tile
list in the SC before OS-008 begins.

If current GORC project parsing rejects these LandCore-specific fields, write
the same facts into `orchestration/docs/data-product-spec.md` and keep
`orchestration/projects/landcore-field-crop-year.project.json` limited to
schema-valid project fields. Record that schema limitation in
`orchestration/docs/issues.md`.

Drive IDs and URLs are allowed in the project document because they are endpoint
identifiers, not credentials. Do not include access keys, rendered rclone config,
OAuth tokens, refresh tokens, service-account JSON, or local private paths.

### `orchestration/docs/data-product-spec.md`

Must define:

```text
field_crop_year_counts.csv
field_crop_year_summary.csv
```

with columns and invariants.

Must also state that production rows are intended only for the SC's
`states_of_interest` and `tiles_of_interest` unless a later SC revision expands
scope.

### `orchestration/docs/STATE.md`

Initial state table:

```md
| Slice | Status | Notes |
|---|---|---|
| OS-001 | implemented | layout and product contract |
| OS-002 | proposed | Python script contracts |
...
```

### `orchestration/docs/issues.md`

Start with:

```md
# Issues

No open issues yet.
```

## Validation

```bash
find orchestration -maxdepth 3 -type f | sort
grep -R "GORC is a separate" orchestration/README.md
grep -R "field_crop_year_counts.csv" orchestration/docs/data-product-spec.md
grep -R "landcore-etl-pipelines.git" orchestration/projects orchestration/docs/data-product-spec.md
grep -R "states_of_interest" orchestration/projects orchestration/docs/data-product-spec.md
grep -R "1YmFECConwSlAFEaMDzyL_srhwVfeTRBy" orchestration/projects orchestration/docs/data-product-spec.md
grep -R "1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4" orchestration/projects orchestration/docs/data-product-spec.md
```

## Completion Criteria

- Folder layout exists.
- Product contract exists.
- GORC version pin file exists.
- LandCore workflow repository, data catalog repository, states of interest,
  pilot tile scope, Yan/Roy Google Drive source file ID, and tile-field-year-crop
  Google Drive publication folder ID are recorded.
- No customer-private data is added.
- No GORC repo files are modified.
