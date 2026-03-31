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
- Current next step is using the staged Lobell tillage field-year output in downstream model-input assembly.
