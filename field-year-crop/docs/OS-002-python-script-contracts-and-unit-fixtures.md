# OS-002: Python Script Contracts and Unit Fixtures

Status: Proposed
Scope: LandCore repository only
Do not modify: GORC core, GORC worker, GORC controller, GORC data-asset providers, GORC geospatial plugins

Recommended model: 5.4-mini
Recommended reasoning: medium


## Purpose

Implement the LandCore-side Python script layer with deterministic synthetic tests, without invoking GORC yet.

This slice creates the scripts that later GORC workflow steps will run.

## Allowed Files

```text
field-year-crop/scripts/python/field_crop_common.py
field-year-crop/scripts/python/discover_raster_asset.py
field-year-crop/scripts/python/write_geospatial_request.py
field-year-crop/scripts/python/summarize_field_crop_counts.py
field-year-crop/tests/fixtures/counts/field_crop_counts_2010.csv
field-year-crop/tests/fixtures/counts/expected_field_crop_year_summary_2010.csv
field-year-crop/tests/test_summarize_field_crop_counts.py
field-year-crop/scripts/smoke/test_python_contracts.sh
field-year-crop/docs/STATE.md
field-year-crop/docs/issues.md
```

## Script Requirements

### `field_crop_common.py`

Provide reusable helpers:

```python
read_csv_rows(path)
write_csv_rows(path, fieldnames, rows)
safe_int(value, field_name)
safe_float(value, field_name)
sha256_file(path)
ensure_parent_dir(path)
```

No third-party dependencies.

### `summarize_field_crop_counts.py`

Inputs:

```text
--counts-csv <path>
--year <int>
--output-csv <path>
--metadata-json <path>
```

Reads a counts CSV with:

```text
field_id,crop_id,count
```

Writes:

```text
field_id,year,crop_id,pixel_count,total_field_pixels,share,is_dominant,dominant_crop_id,dominant_crop_share
```

Rules:

- `field_id`, `crop_id`, and `count` must parse as non-negative integers.
- `year` must be copied to every row.
- Rows sorted by `field_id`, then `crop_id`.
- Dominant crop is max count; ties broken by smallest `crop_id`.
- Share is `count / total_field_pixels`.
- Metadata JSON includes input row count, output row count, distinct field count, year, input SHA-256, and output SHA-256.

### `discover_raster_asset.py`

Inputs:

```text
--asset-path <path>
--output-json <path>
```

Behavior:

- If `asset-path` is a file, treat it as candidate raster path.
- If `asset-path` is a directory, recursively discover candidate rasters by extension:
  - `.tif`
  - `.tiff`
  - `.img`
  - `.bil`
  - `.hdr`
- Prefer non-auxiliary raster files.
- If exactly one candidate exists, write JSON with `raster_path`.
- If multiple candidates exist, write an error and require a future explicit selector.
- Do not require GDAL in this slice.

### `write_geospatial_request.py`

Creates request JSON files for later `goet-geospatial` calls.

Supported request types:

```text
raster_info
align_to_grid
raster_pair_value_counts
```

This script only writes JSON. It does not execute GDAL.

Use the existing operation names exactly. In planning shorthand, "get extents"
means `raster_info`, and "align raster" means `align_to_grid`. Do not add a
LandCore-specific crop-by-raster-extent request type in this slice. The core
workflow uses `align_to_grid` with `like_raster` to produce the CDL raster on
the Yan/Roy tile grid.

## Unit Fixture

Input fixture:

```csv
field_id,crop_id,count
1,5,3
2,1,3
3,2,2
3,4,1
```

Expected summary:

```csv
field_id,year,crop_id,pixel_count,total_field_pixels,share,is_dominant,dominant_crop_id,dominant_crop_share
1,2010,5,3,3,1.000000,true,5,1.000000
2,2010,1,3,3,1.000000,true,1,1.000000
3,2010,2,2,3,0.666667,true,2,0.666667
3,2010,4,1,3,0.333333,false,2,0.666667
```

## Validation

```bash
python3 field-year-crop/tests/test_summarize_field_crop_counts.py
bash field-year-crop/scripts/smoke/test_python_contracts.sh
```

## Completion Criteria

- Summary script is deterministic.
- Fixture output exactly matches expected output.
- No GORC runtime required.
- No GORC repo files are modified.
