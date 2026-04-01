# LandCore ETL Pipelines

This repository contains LandCore-specific ETL pipelines and scripts.

## Layout

- `pipelines/`
- `scripts/`

## Current contents

- Initial import includes Yanroy-related assets from `research-etl`.
- Lobell corn yield pipelines are complete for both:
  - raw download staging
  - `tile_field_ID` + `year` field-year output construction
- Lobell tillage pipelines are complete for both:
  - raw download staging
  - `tile_field_ID` + `year` field-year output construction with tillage counts and proportions
- SSURGO MUKEY staging is complete for:
  - CONUS MUKEY raster download
  - YanRoy `tile_field_id <-> mukey` relationship construction
  - weighted field-level overlap metrics (`pct_overlap`, `overlap_area`)
- The current SSURGO blocker is NCCPI extraction:
  - the local `gSSURGO_CONUS.gdb` contains `Valu1` metadata
  - but the current HPCC environment only exposes the `OpenFileGDB` driver
  - `OpenFileGDB` does not currently expose `Valu1` rows for direct extraction
  - the SDA path is also blocked because the SDA endpoint rejects `FROM valu1`
- Current next step is obtaining a local flat extract of `Valu1` with:
  - `mukey`
  - `nccpi3all`
  - `nccpi3corn`
  - `nccpi3soy`
- Once `Valu1` is staged locally, the next downstream step is using:
  - Lobell corn field-year
  - Lobell tillage field-year
  - SSURGO weighted NCCPI by `tile_field_id`
  - PRISM VPDMAX field-year
  in downstream tillage model-input assembly.
