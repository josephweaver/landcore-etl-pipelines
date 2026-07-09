# OS-005: Real Local Field-Crop-Year Product

Status: Proposed
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
field-year-crop/scripts/python/run_geospatial_pair_counts.py
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
5. Run goet-geospatial align_to_grid with source_raster=CDL 2010 and like_raster=Yan/Roy h18v07.
6. Run goet-geospatial raster_pair_value_counts with field_raster=Yan/Roy h18v07 and value_raster=aligned CDL.
7. Summarize counts with year=2010 and tile=h18v07.
8. Validate summary.
9. Publish outputs to local named location.
```

Do not implement custom raster extent extraction, custom GDAL crop commands, or
new geospatial operations in this slice. `raster_info` provides extent evidence.
`align_to_grid` is the crop/reproject/resample step because `like_raster`
targets the Yan/Roy tile grid.

## Alignment Requirements

Use `goet-geospatial` operation:

```text
align_to_grid
```

Required behavior:

```text
source_raster = CDL 2010
like_raster = Yan/Roy h18v07
resampling = nearest
```

This operation must be used instead of a separate `crop(cdl, yanroy_extent)`
step. The existing plugin uses the `like_raster` grid to produce a tile-sized
aligned CDL raster.

Output:

```text
aligned/cdl_2010_on_h18v07_grid.tif
aligned/cdl_2010_on_h18v07_grid.metadata.json
```

## Pair Count Requirements

Use `goet-geospatial` operation:

```text
raster_pair_value_counts
```

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

Required options:

```json
{
  "require_aligned_grid": true,
  "chunk_rows": 1024,
  "field_dtype": "uint16",
  "value_dtype": "uint16"
}
```

Run this operation after alignment only. If the metadata says the aligned CDL
and Yan/Roy tile grids differ, stop and record a blocker instead of counting.

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
- no field ID exceeds the currently supported dtype range.

## Required Stop Condition: field_id > 65535

If the Yan/Roy raster contains field IDs above 65535, stop and record:

```text
Current GORC geospatial raster_pair_value_counts supports uint16 field IDs only. Real Yan/Roy field IDs exceed uint16.
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
metadata from `raster_pair_value_counts`.

## Validation Command

```bash
bash field-year-crop/scripts/smoke/local_field_crop_year_2010.sh
```

## Completion Criteria

- First real local field-crop-year product exists.
- Validation passes.
- Output row counts and summary statistics are documented.
- Runbook includes exact runtime path and disk usage notes.
- No GORC core/plugin files are modified.
