# Delivery Format

OS-008 packages field-crop-year outputs as a delivery directory, not as an
automatic upload.

Required delivery files:

```text
field_crop_year_counts_all.csv
field_crop_year_summary_all.csv
merge_metadata.json
delivery_manifest.json
gdrive_publish_plan.json
delivery_validation.json
tile-field-year-crop-delivery.zip
```

`field_crop_year_counts_all.csv` columns:

```text
field_id,crop_id,year,tile,count
```

`field_crop_year_summary_all.csv` columns:

```text
field_id,year,tile,crop_id,pixel_count,total_field_pixels,share,is_dominant,dominant_crop_id,dominant_crop_share
```

`delivery_manifest.json` is the package authority. It records source
repositories, selected work units, the intended Google Drive publication folder,
and SHA-256 hashes for package files.

`gdrive_publish_plan.json` lists source package paths and target object paths
for the configured Drive folder. In `publication_mode=plan_only`, it is a plan
only. In `publication_mode=commit_gdrive`, GORC publishes
`tile-field-year-crop-delivery.zip` to the configured rclone path under
`Data/ETL/tile-field-year-crop/<run-id>/`.
