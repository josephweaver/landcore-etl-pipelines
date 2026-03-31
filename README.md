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
- Lobell tillage download pipeline is working for raw staging from the shared LandCore Google Drive.
- Current next step is Lobell tillage field-year construction from the raw rasters.
