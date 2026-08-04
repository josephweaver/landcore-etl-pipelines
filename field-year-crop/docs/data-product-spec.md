# Field-Crop-Year Data Product Specification

This document defines the public product outputs for the LandCore field-crop-year workflow.

## Outputs

### `field_crop_year_counts.csv`

Required columns (in order):

- `field_id`
- `crop_code`
- `year`
- `state`
- `tile`
- `field_pixel_count`
- `crop_pixel_count`
- `crop_fraction`

Invariants:

- `field_id`, `crop_code`, `year`, `state`, and `tile` are required.
- `field_pixel_count`, `crop_pixel_count` are non-negative integers.
- `crop_fraction = crop_pixel_count / field_pixel_count` when `field_pixel_count > 0`.
- `crop_fraction` values are rounded consistently by the implementation contract.

### `field_crop_year_summary.csv`

Required columns (in order):

- `field_id`
- `state`
- `tile`
- `year`
- `dominant_crop_code`
- `dominant_crop_type`
- `dominant_crop_fraction`
- `field_pixel_count`
- `assignment_status`
- `assignment_policy`

Invariants:

- One row per `(field_id, state, tile, year)` with stable sorting.
- `dominant_crop_fraction` is between 0 and 1.
- `assignment_status` is a stable policy outcome value (`assigned`, `unassigned_low_share`, etc.).

## Scope Constraints

- Production rows are limited to `states_of_interest` and `tiles_of_interest` until a later
  Strategic Concept revision expands scope.
- `states_of_interest`: `IL, MN, WI, OH, SD, IA, IN, MI, MO`
- `tiles_of_interest`: initially `h18v07` for the pilot path.
