# OS-005: Real Local Field-Crop-Year Product

Status: Verified
Scope: LandCore repository only
Do not modify: GORC core, GORC worker, GORC controller, GORC data-asset providers, GORC geospatial plugins

Recommended model: 5.4-mini
Recommended reasoning: high


## Purpose

Produce the first real local LandCore field-crop-year data product for year 2010 using:

```text
Yan/Roy h18v07 raster
CDL 2010 raster
```

This slice graduates from metadata-only to the actual product:

```text
aligned CDL -> field_id,crop_id,count -> field/crop/year summary
```

## Allowed Files

```text
field-year-crop/workflows/local-field-crop-year-2010.workflow.json
field-year-crop/submissions/local-field-crop-year-2010.submission.json
field-year-crop/scripts/python/run_align_to_grid.py
field-year-crop/scripts/python/run_numpy_pair_counts.py
field-year-crop/scripts/python/summarize_field_crop_counts.py
field-year-crop/scripts/python/validate_field_crop_year_product.py
field-year-crop/scripts/smoke/local_field_crop_year_2010.sh
field-year-crop/docs/runbook.md
field-year-crop/docs/STATE.md
field-year-crop/docs/issues.md
```

## Workflow Steps

Required data-state transitions:

```text
1. cache_data/materialize CDL 2010 raster.
2. cache_data/materialize or reference Yan/Roy h18v07 raster.
3. Run goet-geospatial raster_info for Yan/Roy h18v07.
4. Run goet-geospatial raster_info for CDL 2010.
5. Run goet-geospatial align_to_grid with source_raster=CDL 2010 and the explicit Yan/Roy h18v07 grid.
6. Run the LandCore Numpy pair-count worker with field_raster=Yan/Roy h18v07 and value_raster=aligned CDL.
7. Summarize counts with year=2010 and tile=h18v07.
8. Validate summary.
9. Publish outputs to local named location.
```

Do not implement custom raster extent extraction or custom GDAL crop commands in
this slice. `raster_info` provides extent evidence.
`align_to_grid` is the crop/reproject/resample step targeting the Yan/Roy tile
grid.

## Alignment Requirements

Use `goet-geospatial` operation:

```text
align_to_grid
```

Required behavior:

```text
source_raster = CDL 2010
target_crs = Yan/Roy h18v07 CRS WKT
target_transform = Yan/Roy h18v07 geotransform
target_width/target_height = Yan/Roy h18v07 dimensions
resampling = nearest
```

This operation must be used instead of a separate `crop(cdl, yanroy_extent)`
step. OS-005 passes the Yan/Roy grid explicitly to avoid CRS authority-code
misidentification in the current geospatial metadata parser.

Output:

```text
aligned/cdl_2010_on_h18v07_grid.tif
aligned/cdl_2010_on_h18v07_grid.metadata.json
```

## Pair Count Requirements

Use `field-year-crop/scripts/python/run_numpy_pair_counts.py`.

Input:

```text
field_raster = Yan/Roy h18v07
value_raster = aligned CDL 2010
```

Output:

```text
counts/field_crop_counts_2010.csv
counts/field_crop_counts_2010.metadata.json
```

Required behavior:

```text
read aligned rasters in row chunks with GDAL ReadAsArray
pack field_id and crop_id into uint64 keys
aggregate counts with numpy.unique
preserve uint32-compatible Yan/Roy field IDs without remapping
```

Run this worker after alignment only. If the aligned CDL and Yan/Roy tile grids
differ, stop and record a blocker instead of counting.

## Validation Script

`validate_field_crop_year_product.py` must check:

- counts CSV exists and is non-empty;
- summary CSV exists and is non-empty;
- all `year` values equal 2010;
- all counts are positive integers;
- all shares are between 0 and 1;
- shares sum to approximately 1.0 per field;
- dominant crop is deterministic;
- summary row count equals counts row count;
- metadata JSON files exist;
- field IDs are preserved as non-negative uint32-compatible values.

## Required Stop Condition: negative field_id

If the Yan/Roy raster contains negative field IDs, stop and record:

```text
Yan/Roy field IDs must be non-negative for uint32 pair counts.
```

Do not coerce IDs.

## Expected Artifacts

```text
metadata/input_discovery.json
metadata/raster_info.json
aligned/cdl_2010_on_h18v07_grid.tif
aligned/cdl_2010_on_h18v07_grid.metadata.json
counts/field_crop_counts_2010.csv
counts/field_crop_counts_2010.metadata.json
summary/field_crop_year_summary_2010.csv
summary/field_crop_year_summary_2010.metadata.json
validation/field_crop_year_validation_2010.json
```

The metadata artifacts should make the plugin reuse visible: raster info
metadata for each input, alignment metadata from `align_to_grid`, and count
metadata from the Numpy pair-count worker.

## Validation Command

```bash
bash field-year-crop/scripts/smoke/local_field_crop_year_2010.sh
```

Verified locally against h18v07 2010:

```text
counts rows: 221084
summary rows: 221084
distinct fields: 47922
total counted pixels: 20902248
max field_id: 47922
pair-count method: numpy_unique_uint64_pair_key
pair-count elapsed seconds: 7.731
```

## Completion Criteria

- First real local field-crop-year product exists.
- Validation passes.
- Output row counts and summary statistics are documented.
- Runbook includes exact runtime path and disk usage notes.
- No GORC core/plugin files are modified.
