# OS-008: Production Tiling, Delivery, and Provenance

Status: Proposed
Scope: LandCore repository only
Do not modify: GORC core, GORC worker, GORC controller, GORC data-asset providers, GORC geospatial plugins

Recommended model: 5.4-mini
Recommended reasoning: medium


## Purpose

After the real HPCC one-tile run succeeds, define the production expansion path for multiple tiles and/or years and package outputs for delivery.

This slice should still be conservative. It prepares production mechanics without changing GORC.

## Allowed Files

```text
field-year-crop/workflows/production-field-crop-year.workflow.json
field-year-crop/submissions/production-field-crop-year.template.json
land-core.project.json
field-year-crop/scripts/python/merge_field_crop_year_outputs.py
field-year-crop/scripts/python/write_delivery_manifest.py
field-year-crop/scripts/python/validate_delivery_package.py
field-year-crop/scripts/python/write_gdrive_publish_plan.py
field-year-crop/scripts/smoke/production_dry_run.sh
field-year-crop/docs/delivery-format.md
field-year-crop/docs/production-runbook.md
field-year-crop/docs/runbook.md
field-year-crop/docs/STATE.md
field-year-crop/docs/issues.md
```

## Production Workflow Shape

Production should derive work from explicit years and `tiles_of_interest`.
Do not infer production work units from scanning private directories in the
first production slice.

The intended DAG shape is:

```text
cache_data(cdl_year)
cache_data(yanroy_tile)
  -> raster_info(yanroy_tile)
  -> raster_info(cdl_year)
  -> align_to_grid(cdl_year, like_raster=yanroy_tile)
  -> raster_pair_value_counts(yanroy_tile, aligned_cdl_year_tile)
  -> summarize_field_crop_counts(year, tile)
  -> validate_field_crop_year_product(year, tile)
merge year-tile outputs
write delivery_manifest.json
write gdrive_publish_plan.json
```

Fanout policy:

```text
cache_data(cdl_year): one per year, deduplicated by cache key
cache_data(yanroy_tile): one per tile in tiles_of_interest
raster_info(yanroy_tile): one per tile
align_to_grid: one per year x tile
raster_pair_value_counts: one per year x tile
summarize/validate: one per year x tile
merge: one per production run
```

Production should use an explicit list of year-tile work units:

```json
[
  {"year": 2010, "tile": "h18v07"},
  {"year": 2011, "tile": "h18v07"}
]
```

Each work unit consumes the cached CDL asset for its year and the cached Yan/Roy
asset for its tile.

## Geospatial Plugin Reuse

Production must reuse existing `goet-geospatial` operations:

```text
raster_info
align_to_grid
raster_pair_value_counts
```

`raster_info` is the required source of Yan/Roy tile extent evidence. Do not
write a LandCore-specific `get_extents` raster parser.

`align_to_grid` is the required CDL tile preparation step:

```text
source_raster = CDL year raster
like_raster = Yan/Roy tile raster
resampling = nearest
```

This call is the core crop/reproject/resample operation for CDL. Do not add a
separate `crop(cdl.path, yanroy_extent)` step unless the existing
`align_to_grid` plugin cannot produce a tile-sized CDL raster; if that happens,
record the blocker in `field-year-crop/docs/issues.md` instead of adding custom
GDAL commands.

`raster_pair_value_counts` must run after alignment with:

```json
{
  "require_aligned_grid": true,
  "chunk_rows": 1024,
  "field_dtype": "uint16",
  "value_dtype": "uint16"
}
```

The existing `crop_by_polygons` plugin is not part of the core production path.
Use it only in a later slice if production introduces vector polygon AOIs.

## LandCore Production Scope

Production work units must be derived from explicit state and tile lists, not
from directory scans.

Allowed states:

```text
IL, MN, WI, OH, SD, IA, IN, MI, MO
```

Allowed tiles are the `tiles_of_interest` listed in
`SC-landcore-field-crop-year-data-product.md`. The production template may use
a smaller explicit subset for dry runs, but validation must fail if a work unit
contains a tile outside that SC list.

The production workflow or project file must carry these facts:

```json
{
  "source_repositories": {
    "landcore_etl_pipelines": "https://github.com/josephweaver/landcore-etl-pipelines.git",
    "landcore_data_catalog": "https://github.com/land-core/landcore-data-catalog.git"
  },
  "states_of_interest": ["IL", "MN", "WI", "OH", "SD", "IA", "IN", "MI", "MO"],
  "tiles_of_interest_source": "field-year-crop/docs/SC-landcore-field-crop-year-data-product.md",
  "google_drive_endpoints": {
    "yanroy_release_file_id": "1YmFECConwSlAFEaMDzyL_srhwVfeTRBy",
    "tile_field_year_crop_publish_folder_id": "1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4"
  }
}
```

## Required Outputs

Per work unit:

```text
field_crop_year_counts_<year>_<tile>.csv
field_crop_year_summary_<year>_<tile>.csv
metadata_<year>_<tile>.json
alignment_<year>_<tile>.metadata.json
pair_counts_<year>_<tile>.metadata.json
validation_<year>_<tile>.json
```

Merged output:

```text
field_crop_year_counts_all.csv
field_crop_year_summary_all.csv
delivery_manifest.json
gdrive_publish_plan.json
```

`gdrive_publish_plan.json` must describe what will be uploaded to the Google
Drive folder, without performing the upload during the dry run:

```json
{
  "schema": "landcore/tile-field-year-crop-gdrive-publish-plan/v1",
  "target_folder_id": "1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4",
  "target_folder_url": "https://drive.google.com/drive/folders/1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4?usp=drive_link",
  "source_delivery_manifest": "delivery_manifest.json",
  "objects": []
}
```

## Delivery Manifest

`delivery_manifest.json` must include:

```json
{
  "schema": "landcore/field-crop-year-delivery/v1",
  "created_at": "<UTC timestamp>",
  "gorc_repository": "https://github.com/josephweaver/go-etl",
  "gorc_commit": "<commit>",
  "workflow": "<workflow path>",
  "landcore_repository": "https://github.com/josephweaver/landcore-etl-pipelines.git",
  "landcore_data_catalog_repository": "https://github.com/land-core/landcore-data-catalog.git",
  "yanroy_release_drive_file_id": "1YmFECConwSlAFEaMDzyL_srhwVfeTRBy",
  "tile_field_year_crop_publish_drive_folder_id": "1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4",
  "tile_field_year_crop_publish_drive_url": "https://drive.google.com/drive/folders/1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4?usp=drive_link",
  "states_of_interest": ["IL", "MN", "WI", "OH", "SD", "IA", "IN", "MI", "MO"],
  "tiles_of_interest": [],
  "work_units": [],
  "outputs": [],
  "publication": {
    "target": "google_drive_folder",
    "status": "planned",
    "folder_id": "1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4",
    "uploaded_objects": []
  },
  "validation": {}
}
```

## Google Drive Publication

The intended durable destination for the finished tile-field-year-crop product is:

```text
https://drive.google.com/drive/folders/1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4?usp=drive_link
folder ID: 1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4
```

If GORC does not yet support `commit_data` to `gdrive_rclone`, this slice should
produce a deterministic publish plan and document a manual or LandCore-side
post-processing command such as `rclone copy` from the delivery package to the
configured Drive folder. Do not modify GORC to add outbound Google Drive support
inside this LandCore slice.

The publication command must be gated by explicit operator action and must not
run during `production_dry_run.sh`.

## Validation Rules

Production validation must check:

- no duplicate `(field_id, crop_id, year, tile)` rows in merged counts;
- no duplicate `(field_id, crop_id, year, tile)` rows in merged summary;
- all expected work units are present;
- every work unit uses an allowed state when state is present;
- every work unit uses an allowed tile from the SC tile list;
- every year-tile work unit has `raster_info`, `align_to_grid`, and
  `raster_pair_value_counts` metadata;
- every `raster_pair_value_counts` request used `require_aligned_grid=true`;
- all per-work-unit validations passed;
- delivery manifest hashes match files;
- `gdrive_publish_plan.json` references only files listed in
  `delivery_manifest.json`;
- the Google Drive publication folder ID equals
  `1yu6bx8ZvJTKX0KIC2Nfzuys-wOgjMGu4`;
- no private input rasters are copied into the delivery package unless explicitly requested.

## Production Dry Run

`production_dry_run.sh` should run against synthetic or tiny fixture work units first.

Do not run full production automatically.

## Completion Criteria

- Production workflow template exists.
- Merge script exists.
- Delivery manifest writer exists.
- Google Drive publish plan writer exists.
- Delivery docs exist.
- Production state and tile scope validation exists.
- The delivery manifest records the intended Google Drive publication folder.
- Dry run passes on tiny fixture data.
- Full production remains manual and gated by a human review.
