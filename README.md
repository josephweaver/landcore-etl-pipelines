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
- Current next step is Lobell tillage ingestion, starting with raw download from LandCore Google Drive.
