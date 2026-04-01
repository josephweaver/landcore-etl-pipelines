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
- SSURGO progress:
  - CONUS MUKEY raster download is complete
  - YanRoy `tile_field_id <-> mukey` relationship construction is complete
  - output contract for `tile_field2mukey.csv` is:
    - `tile_field_id`
    - `mukey`
    - `pct_overlap`
    - `overlap_area`
  - state-level gSSURGO download pipeline is now active for:
    - `IA`, `IL`, `IN`, `MI`, `MN`, `MO`, `OH`, `SD`, `WI`
  - state-level `Valu1` NCCPI extraction pipeline now exists and targets:
    - `mukey`
    - `nccpi3all`
    - `nccpi3corn`
    - `nccpi3soy`
- SSURGO implementation notes:
  - the older SDA approach is deprecated because the SDA endpoint rejects `FROM valu1`
  - the CONUS `gSSURGO_CONUS.gdb` path is not sufficient in the current HPCC environment because `OpenFileGDB` does not expose `Valu1` rows directly
  - the current active path is the state-level gSSURGO workflow plus per-state `Valu1` extraction
  - the state-level NCCPI extraction pipeline is currently in active runtime validation/debugging on HPCC
- Current next SSURGO step is to finish validating `state_valu1_nccpi_extract.yml`, then use the extracted MUKEY-level NCCPI table to build weighted field-level NCCPI by `tile_field_id`.
- Once SSURGO weighted NCCPI is complete, the next downstream step is using:
  - Lobell corn field-year
  - Lobell tillage field-year
  - SSURGO weighted NCCPI by `tile_field_id`
  - PRISM VPDMAX field-year
  in downstream tillage model-input assembly.
