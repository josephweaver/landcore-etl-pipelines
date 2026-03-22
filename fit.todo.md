# Fit Pipeline Plan (YanRoy field-level replacement for RiskModel-HPCC)

## Progress Notes (2026-02-23)
- Completed today:
  - Reworked SSURGO field->mukey mapper for real YanRoy tile-file inputs:
    - derives `tile_field_id` when missing,
    - supports field and SSURGO glob fallbacks,
    - records many-to-many overlaps via `pct_field_overlap`.
  - Recreated SSURGO download pipelines:
    - `pipelines/ssurgo/state_download.yml` (manual URL list workflow),
    - `pipelines/ssurgo/conus_download.yml` (single CONUS URL workflow).
  - Added deterministic HTTP output filename support (`out_file`) in `web_download_list`; CONUS pipeline now writes `{rawdir}/ssurgo_conus.zip`.
  - Updated SSURGO glob defaults to include `.gdb`, `.gpkg`, and likely mapunit `.shp` files.
- Current blocker:
  - `pipelines/ssurgo/yanroy_nccpi_sda.yml` step 1 still requires local SSURGO polygon data in `{env.basedir}/data/ssurgo/state/extract`; SDA step 2 is not the failing component.
- Immediate next actions:
  - Run `pipelines/ssurgo/conus_download.yml` (or `state_download.yml`) to populate local SSURGO extracts.
  - Rerun `pipelines/ssurgo/yanroy_nccpi_sda.yml` step 0 then step 1.
  - Verify `field_mukey_pairs.csv` overlap percentages before NCCPI joins.

## Objective
Replace:
- `RiskModel-HPCC/controller/controller.R`
- `RiskModel-HPCC/src/Neighborhood_fit.R`

with Research ETL pipelines that build model-ready data from:
- Lobell corn + tillage
- SSURGO NCCPI
- PRISM `vpdmax`
- YanRoy field IDs (field-level aggregation key)

and execute neighborhood model fitting reproducibly on ETL executors.

## Assumptions (confirm)
- "YarRoy" means `YanRoy` field ID assets already used in this repo.
- Primary join key should be `tile_field_ID` (e.g., `hXXvYY_<field_id>`), with yearly rows per field for model input.
- Target modeling unit remains county-FIPS neighborhoods (as in old controller), but observations are field-year rows.
- We keep BRMS model logic initially (port to a script invoked by ETL), then optimize later.
- Legacy ETL reference repo for source behavior: `../RiskModel-etl/R`.
- TerraClimate work is paused for now; climate path for this milestone is PRISM `vpdmax`.
- State scope for this milestone is fixed to states-of-interest:
  - `IL, MN, WI, OH, SD, IA, IN, MI, MO`
  - implemented operationally via `states.of.interest.csv` -> `tiles.of.interest.csv` (YanRoy flow).
- Start point for dependency graph is YanRoy (`pipelines/yanroy/*`) and tile filtering.

## Old -> New Responsibility Mapping
- Old `controller.R`: target county selection, neighborhood county expansion, per-county job materialization, SLURM submit.
- New ETL: dependency pipelines create canonical feature tables; orchestrator pipeline fans out county-neighborhood fit jobs via `pipeline_execute.py` + executor.

- Old `Neighborhood_fit.R`: scaling, derived covariates (`mean_RCI`, `within_RCI`), informed priors, BRMS fit, model artifact output.
- New ETL: `scripts/model/fit_neighborhood_brms.R` (or python wrapper + R script) run by `exec_script.py` in a child pipeline.

## Decoupled Pipeline Plan

Guiding rule for this plan:
- Prefer one canonical dataset per pipeline.
- Keep source extraction, canonicalization, joins, and model execution in separate pipelines.
- Let downstream pipelines depend only on named dataset outputs, not on raw folders when we can avoid it.

### A) Field key / geometry foundation
1. `pipelines/risk_model/yanroy_field_index.yml`
   - dataset: `stage.yanroy_field_index_v1`
   - purpose: canonical field keys and geometry references
   - grain: one row per field
   - required columns:
     - `tile_coord`
     - `field_ID`
     - `tile_field_ID`
     - geometry reference and/or polygon path
     - `FIPS` if available at this stage

### B) Lobell source datasets
2. `pipelines/lobell/corn_field_year.yml`
   - dataset: `stage.lobell_corn_yanroy_field_year_v1`
   - grain: one row per field-year
   - columns:
     - `tile_coord`
     - `field_ID`
     - `tile_field_ID`
     - `year`
     - `unscaled_yield`
   - lineage: historical `0210` extract + `0310` combine
   - current implementation path:
     - `pipelines/lobell/corn_download.yml`
     - `pipelines/lobell/corn_field_year.yml`
     - `scripts/model/aggregate_lobell_corn_to_fields.py`

3. `pipelines/lobell/tillage_field_year.yml`
   - dataset: `stage.lobell_tillage_field_year_v1`
   - grain: one row per field-year
   - columns:
     - `tile_coord`
     - `field_ID`
     - `tile_field_ID`
     - `year`
     - `dominant_tillage`
     - optional tillage shares / QA fields
   - lineage: historical `0208` extract + `0308` combine

4. `pipelines/lobell/soy_field_year.yml`
   - dataset: `stage.lobell_soy_field_year_v1`
   - grain: one row per field-year
   - columns:
     - `tile_coord`
     - `field_ID`
     - `tile_field_ID`
     - `year`
     - `soy_yield_mean`
   - lineage: historical `0209` extract + `0309` combine

5. `pipelines/lobell/field_year.yml`
   - dataset: `stage.lobell_field_year_v1`
   - purpose: canonical merged Lobell field-year table for modeling
   - inputs:
     - `lobell_corn_field_year`
     - `lobell_tillage_field_year`
     - optional `lobell_soy_field_year`
   - columns:
     - `tile_field_ID`
     - `year`
     - `unscaled_yield`
     - `dominant_tillage`
     - optional soy fields

### C) Soil and climate datasets
6. `pipelines/risk_model/ssurgo_nccpi_field.yml`
   - dataset: `stage.ssurgo_field_nccpi_v1`
   - grain: one row per field
   - columns:
     - `tile_field_ID`
     - `NCCPI`
     - optional `nccpi3corn`, `nccpi3soy`, `nccpi3all`
     - overlap metadata
   - note: existing `pipelines/ssurgo/yanroy_nccpi_sda.yml` is already the main precursor here

7. `pipelines/prism/vpdmax_field_year.yml`
   - dataset: `stage.prism_vpdmax_field_year_v1`
   - grain: one row per field-year
   - columns:
     - `tile_field_ID`
     - `year`
     - `vpdmax_7`
     - `vpdmax_8`

### D) Model-ready covariates
8. `pipelines/risk_model/field_covariates_tillage_model.yml`
   - dataset: `stage.field_covariates_tillage_model_v1`
   - inputs:
     - `yanroy_field_index`
     - `lobell_field_year`
     - `ssurgo_nccpi_field`
     - `prism_vpdmax_field_year`
   - grain: one row per field-year
   - required columns:
     - `tile_field_ID`
     - `FIPS`
     - `year`
     - `unscaled_yield`
     - `dominant_tillage`
     - `NCCPI`
     - `vpdmax_7`
     - `vpdmax_8`

### E) County neighborhood datasets
9. `pipelines/risk_model/county_neighborhood_targets.yml`
   - dataset: `stage.county_neighborhood_targets_v1`
   - purpose: target county list and adjacency expansion
   - outputs:
     - `target_counties.csv`
     - `county_neighbors.csv`

10. `pipelines/risk_model/county_model_input_tillage.yml`
   - dataset family: `stage.county_model_input_tillage_v1`
   - purpose: materialize one county-neighborhood model input per focal county
   - inputs:
     - `field_covariates_tillage_model`
     - `county_neighborhood_targets`

### F) Model execution
11. `pipelines/risk_model/neighborhood_fit_child.yml`
   - artifact family: `model.landcore_tillage_neighborhood_fit_child_v1`
   - purpose: run one county BRMS fit using tillage terms instead of RCI terms

12. `pipelines/risk_model/neighborhood_fit_parent.yml`
   - purpose: orchestration/fanout only
   - note: avoid embedding data-building logic here

13. `pipelines/risk_model/neighborhood_fit_manifest.yml`
   - dataset: `model.landcore_tillage_neighborhood_fit_manifest_v1`
   - purpose: collect model outputs and fit metadata into one manifest

## Legacy Script Lineage (for parity)
- Lobell corn tiles: `../RiskModel-etl/R/lobell/0210-extract-lobell-corn-tiles.R`
- Lobell tillage tiles: `../RiskModel-etl/R/lobell/0208-extract-lobell-tillage-tiles.R`
- SSURGO all tiles: `../RiskModel-etl/R/0205-extract-ssurgo-all-tiles.R`
- TerraClimate extraction: `../RiskModel-etl/R/0204-extract-terraclimate-tiles.R`
- Multi-source tile/year combine: `../RiskModel-etl/R/0300-combine-datasource-tiles-by-tile-year.R`

Important parity note:
- In the legacy `0300-combine-datasource-tiles-by-tile-year.R`, `vpdmax` is sourced from PRISM (`PRISM_var_importer`), not TerraClimate.
- If the new requirement is specifically TerraClimate `vpdmax`, we should treat that as an intentional model-input change and record it in metadata/versioning.

## Detailed TODO

### 0) Scaffolding and configs
- [ ] Create/finalize the decoupled `pipelines/risk_model/` pipeline set listed above.
- [ ] Add model-specific vars in `config/projects.yml` for source paths/URIs.
- [ ] Add/confirm dataset IDs for each stage output (`raw|stage|serve` naming).
- [ ] Add/confirm Lobell raw source vars:
  - `Data/Tillage/Tillage_Data`
  - `Data/Yield/Corn`
  - (Google Drive source in project env vars).
- [ ] Add explicit data-integrity checks TODO for Lobell raw files (completeness, year coverage, schema, missing/corrupt files) before using in model build.

### 1) Field index and geometry canonicalization
Pipeline: `pipelines/risk_model/field_index.yml`
- [ ] Build canonical field key table from YanRoy outputs:
  - `tile_id`, `field_id`, `tile_field_ID`, geometry or geometry reference.
- [ ] Persist `field_index.parquet/csv` and `field_index.summary.json`.
- [ ] Validate uniqueness of `tile_field_ID` and non-null geometry refs.

### 2) Lobell corn + tillage ingest/normalize
Pipelines:
- `pipelines/lobell/corn_download.yml`
- `pipelines/lobell/corn_field_year.yml`
- `pipelines/lobell/tillage_field_year.yml`
- `pipelines/lobell/soy_field_year.yml`
- `pipelines/lobell/field_year.yml`
- [ ] Ingest Lobell inputs from defined source (gdrive/http/local).
- [ ] Convert historical extract-stage logic into ETL fanout over `tile x year`.
- [ ] Normalize each Lobell source to field-year grain independently.
- [ ] Emit canonical per-dataset outputs before the final merged Lobell table.
- [x] First corn ETL path exists:
  - raw download from Google Drive `Data/Yield/Corn`
  - aggregation from raw corn rasters to YanRoy `tile_field_ID` field-year rows
- [ ] Mirror aggregation logic from:
  - `../RiskModel-etl/R/lobell/0210-extract-lobell-corn-tiles.R`
  - `../RiskModel-etl/R/lobell/0208-extract-lobell-tillage-tiles.R`
- [ ] Integrity TODO (must pass before downstream join):
  - year-by-year file presence check,
  - duplicate key check (`tile_field_ID`,`year`),
  - NA/coverage thresholds for yield and tillage outputs.
- [x] ETL conversion of the historical Lobell combine semantics now exists as an initial bridge:
  - `scripts/model/build_lobell_field_year.py`
  - `pipelines/lobell/corn_tillage_ingest.yml`
  - this should be treated as the first bridge artifact, then split into the decoupled pipelines above.
- [ ] Remaining Lobell ETL work:
  - convert the raster extraction stage (`0208/0209/0210`) into ETL fanout over `tile x year`,
  - wire raw-source download/staging into the pipeline inputs,
  - add duplicate-key / coverage validation before downstream joins.

### 3) SSURGO NCCPI ingest/normalize
Pipeline: `pipelines/risk_model/ssurgo_nccpi_field.yml`
- [ ] Ingest SSURGO NCCPI (likely static per field/soil unit).
- [ ] Build field-level NCCPI mapped to `tile_field_ID`.
- [ ] Emit `nccpi_field.parquet/csv`.
- [ ] QA: coverage %, duplicate keys, value ranges.
- [ ] Mirror MUKEY -> field mode-assignment pattern from `../RiskModel-etl/R/0205-extract-ssurgo-all-tiles.R`.
- [ ] Assume we fetch/build our own gSSURGO source tables for this repo (do not rely on prebuilt legacy artifacts).
- [ ] Include Valu1 coverage for corn/soy/all as requested:
  - reference: `https://www.nrcs.usda.gov/sites/default/files/2022-08/gSSURGO%20Value%20Added%20Look%20Up%20Valu1%20Table%20Column%20Descriptions.pdf`
  - initial extraction should retain needed Valu1 columns so we can derive NCCPI variants without re-extraction.
- [x] Build/validate field<->mukey crosswalk script with percent field overlap for many-to-many relationships.
- [x] Create SSURGO download pipelines for both manual state URL lists and single CONUS HTTP archive.

### 4) PRISM vpdmax ingest/aggregate (active)
Pipelines:
- `pipelines/prism/vpdmax_download.yml`
- `pipelines/prism/vpdmax_field_year.yml`
- [ ] Download/stage PRISM monthly `vpdmax` rasters for model years.
- [ ] Aggregate PRISM `vpdmax` to YanRoy fields for month 7 and 8 at minimum:
  - outputs: `vpdmax_7`, `vpdmax_8` by `tile_field_ID`, `year`.
- [ ] Emit `vpdmax_field_year.parquet/csv` + QA stats.
- [ ] Reuse pattern from existing PRISM pipeline structure (`pipelines/prism/download.yml`) but parameterized for `vpdmax`.
- [ ] Add validation checks:
  - expected monthly file count per year (12 or documented exceptions),
  - no missing `vpdmax_7`/`vpdmax_8` for retained model rows.

### 4.1) TerraClimate hold (future)
- [ ] Keep TerraClimate extraction tasks out of current milestone.
- [ ] Revisit only after PRISM-based model replacement is stable.

### 5) Unified field covariates table
Pipeline: `pipelines/risk_model/field_covariates_tillage_model.yml`
- [ ] Join Lobell + NCCPI + VPD to one canonical table:
  - key: `tile_field_ID`, `year`
  - columns needed by fit milestone: `unscaled_yield`, `dominant_tillage` (or equivalent tillage term), `NCCPI`, `vpdmax_7`, `vpdmax_8`, `FIPS`.
- [ ] Add strict validation:
  - no duplicate (`tile_field_ID`,`year`)
  - required column null thresholds
  - year range checks
- [ ] Emit `field_covariates_v1.parquet/csv`.

### 6) County neighborhood prep
Pipeline: `pipelines/risk_model/county_neighborhood_targets.yml`
- [ ] Ingest/maintain county adjacency table (current `county_adjacency2010.csv`).
- [ ] Recreate target county selection logic from old controller:
  - `min_n >= 500`
  - allowed states set
  - optional VPD filter (`max mean vpdmax_7 excluding 2012 > 21`).
- [ ] Emit:
  - `target_counties.csv`
  - `county_neighbors.csv`.

### 7) Model input materialization by target county
Pipeline: `pipelines/risk_model/county_model_input_tillage.yml`
- [ ] For each target county, build neighborhood training extract (focal + neighbors).
- [ ] Output one file per county:
  - `model_input/<FIPS>/county_data.csv`
- [ ] Include metadata sidecar per county (row count, year range, missingness).

### 8) Child model fit pipeline (replacement for `Neighborhood_fit.R` runtime)
Pipeline: `pipelines/risk_model/neighborhood_fit_child.yml`
- [x] Add script `scripts/model/fit_neighborhood_brms.R`:
  - starts from the current BRMS structure, replaces RCI decomposition with `mean_tillage` / `within_tillage`, and keeps random effect `(1|tile_field_ID)`.
- [x] Add `pipelines/risk_model/neighborhood_fit_child.yml` to run a normalize-input step plus `Rscript` fit for one county input.
- [ ] Run on one county input and confirm package/runtime availability on the target executor.
- [ ] Emit artifacts:
  - `output_model.rds`
  - `fit_summary.json`
  - diagnostics/logs.

### 9) Parent orchestration pipeline (replacement for `controller.R`)
Pipeline: `pipelines/risk_model/neighborhood_fit_parent.yml`
- [ ] Fan out across `target_counties` and call child via `pipeline_execute.py`.
- [ ] Support `mode=synchronized` for deterministic completion.
- [ ] Executor profile for HPCC (`slurm` or `hpcc_direct`) with retries.

### 10) Model collection and registry
Pipeline: `pipelines/risk_model/neighborhood_fit_manifest.yml`
- [ ] Combine per-county fit metadata into run-level manifest.
- [ ] Optional: register/store model artifacts with dataset IDs.
- [ ] Emit summary table for downstream scoring.

## Data Contract (minimum model input schema)
Per row (`tile_field_ID`, `year`):
- `tile_field_ID` (string)
- `FIPS` (string)
- `year` (int)
- `unscaled_yield` (numeric)
- `dominant_tillage` (numeric or categorical, to be finalized)
- `NCCPI` (numeric)
- `vpdmax_7` (numeric)
- `vpdmax_8` (numeric)

Derived in fit script:
- (RCI-derived terms removed for this milestone unless temporarily retained for parity test runs)

## Implementation Order (recommended)
1. `yanroy_field_index`
2. `lobell_corn_field_year`
3. `lobell_tillage_field_year`
4. `lobell_field_year`
5. `ssurgo_nccpi_field`
6. `prism_vpdmax_field_year`
7. `field_covariates_tillage_model`
8. `county_neighborhood_targets`
9. `county_model_input_tillage`
10. `neighborhood_fit_child`
11. `neighborhood_fit_parent`
12. `neighborhood_fit_manifest`

## Acceptance Criteria
- [ ] ETL can reproduce old controller behavior without manual scratch-folder job templating.
- [ ] For a test county set, pipeline outputs one model artifact per county FIPS.
- [ ] Model input tables satisfy schema + QA checks and are versioned as datasets.
- [ ] Parent pipeline runs on HPCC executor and records statuses/events in ETL tracking.
- [ ] Milestone criterion met: model spec replaces RCI predictor with tillage-based predictor(s) while preserving neighborhood-fit execution flow.

## Open Questions to Resolve Early
- [ ] Confirm exact tillage variable encoding for model term.
  - Current child-fit implementation assumes numeric `annual_tillage` and decomposes it into `mean_tillage` / `within_tillage`, mirroring the old RCI pattern.
  - If the intended tillage predictor is categorical or proportion-based, adjust the normalization script and BRMS formula before full runs.
- [ ] Confirm SSURGO-to-field crosswalk method for NCCPI (direct overlay vs existing lookup).
- [ ] Confirm PRISM `vpdmax` product/resolution path and target year span for this first cut.
- [ ] Confirm final Valu1 column subset to persist from gSSURGO (`corn/soy/all` families) for model + future features.
