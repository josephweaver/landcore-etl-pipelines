# Fit Pipeline Plan (YanRoy field-level replacement for RiskModel-HPCC)

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

## Proposed Pipeline DAG
1. `pipelines/yanroy/extract_fields.yml` (existing dependency)
2. `pipelines/risk_model/field_index.yml`
3. `pipelines/risk_model/lobell_corn_tillage_ingest.yml`
4. `pipelines/risk_model/ssurgo_nccpi_ingest.yml`
5. `pipelines/prism/vpdmax_download.yml`
6. `pipelines/prism/vpdmax_field_aggregate.yml`
7. `pipelines/risk_model/field_covariates_build.yml`
8. `pipelines/risk_model/county_neighborhood_prep.yml`
9. `pipelines/risk_model/model_input_build.yml`
10. `pipelines/risk_model/neighborhood_fit_parent.yml`
11. `pipelines/risk_model/neighborhood_fit_child.yml`
12. `pipelines/risk_model/model_collect.yml`

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
- [ ] Create `pipelines/risk_model/` folder and base wrappers.
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
Pipeline: `pipelines/risk_model/lobell_corn_tillage_ingest.yml`
- [ ] Ingest Lobell inputs from defined source (gdrive/http/local).
- [ ] Normalize schema to field-year grain:
  - required: `tile_field_ID`, `year`, `unscaled_yield`, tillage fields used in current modeling.
- [ ] Emit `lobell_field_year.parquet/csv` + QA report.
- [ ] Mirror aggregation logic from:
  - `../RiskModel-etl/R/lobell/0210-extract-lobell-corn-tiles.R`
  - `../RiskModel-etl/R/lobell/0208-extract-lobell-tillage-tiles.R`
- [ ] Integrity TODO (must pass before downstream join):
  - year-by-year file presence check,
  - duplicate key check (`tile_field_ID`,`year`),
  - NA/coverage thresholds for yield and tillage outputs.

### 3) SSURGO NCCPI ingest/normalize
Pipeline: `pipelines/risk_model/ssurgo_nccpi_ingest.yml`
- [ ] Ingest SSURGO NCCPI (likely static per field/soil unit).
- [ ] Build field-level NCCPI mapped to `tile_field_ID`.
- [ ] Emit `nccpi_field.parquet/csv`.
- [ ] QA: coverage %, duplicate keys, value ranges.
- [ ] Mirror MUKEY -> field mode-assignment pattern from `../RiskModel-etl/R/0205-extract-ssurgo-all-tiles.R`.
- [ ] Assume we fetch/build our own gSSURGO source tables for this repo (do not rely on prebuilt legacy artifacts).
- [ ] Include Valu1 coverage for corn/soy/all as requested:
  - reference: `https://www.nrcs.usda.gov/sites/default/files/2022-08/gSSURGO%20Value%20Added%20Look%20Up%20Valu1%20Table%20Column%20Descriptions.pdf`
  - initial extraction should retain needed Valu1 columns so we can derive NCCPI variants without re-extraction.

### 4) PRISM vpdmax ingest/aggregate (active)
Pipelines:
- `pipelines/prism/vpdmax_download.yml`
- `pipelines/prism/vpdmax_field_aggregate.yml`
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
Pipeline: `pipelines/risk_model/field_covariates_build.yml`
- [ ] Join Lobell + NCCPI + VPD to one canonical table:
  - key: `tile_field_ID`, `year`
  - columns needed by fit milestone: `unscaled_yield`, `dominant_tillage` (or equivalent tillage term), `NCCPI`, `vpdmax_7`, `vpdmax_8`, `FIPS`.
- [ ] Add strict validation:
  - no duplicate (`tile_field_ID`,`year`)
  - required column null thresholds
  - year range checks
- [ ] Emit `field_covariates_v1.parquet/csv`.

### 6) County neighborhood prep
Pipeline: `pipelines/risk_model/county_neighborhood_prep.yml`
- [ ] Ingest/maintain county adjacency table (current `county_adjacency2010.csv`).
- [ ] Recreate target county selection logic from old controller:
  - `min_n >= 500`
  - allowed states set
  - optional VPD filter (`max mean vpdmax_7 excluding 2012 > 21`).
- [ ] Emit:
  - `target_counties.csv`
  - `county_neighbors.csv`.

### 7) Model input materialization by target county
Pipeline: `pipelines/risk_model/model_input_build.yml`
- [ ] For each target county, build neighborhood training extract (focal + neighbors).
- [ ] Output one file per county:
  - `model_input/<FIPS>/county_data.csv`
- [ ] Include metadata sidecar per county (row count, year range, missingness).

### 8) Child model fit pipeline (replacement for `Neighborhood_fit.R` runtime)
Pipeline: `pipelines/risk_model/neighborhood_fit_child.yml`
- [ ] Add script `scripts/model/fit_neighborhood_brms.R`:
  - start from current formula/priors structure, but replace RCI predictor terms with tillage-based term(s) for this milestone; keep random effect `(1|tile_field_ID)`.
- [ ] Run via `exec_script.py` on one county input.
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
Pipeline: `pipelines/risk_model/model_collect.yml`
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
1. Field index + unified covariates (Steps 1-5)
2. County neighborhood prep + model input materialization (Steps 6-7)
3. Child fit script + single-county dry run
4. Parent fanout on small county set
5. Full run and artifact collection

## Acceptance Criteria
- [ ] ETL can reproduce old controller behavior without manual scratch-folder job templating.
- [ ] For a test county set, pipeline outputs one model artifact per county FIPS.
- [ ] Model input tables satisfy schema + QA checks and are versioned as datasets.
- [ ] Parent pipeline runs on HPCC executor and records statuses/events in ETL tracking.
- [ ] Milestone criterion met: model spec replaces RCI predictor with tillage-based predictor(s) while preserving neighborhood-fit execution flow.

## Open Questions to Resolve Early
- [ ] Confirm exact tillage variable encoding for model term (binary dominant tillage vs proportion-based features).
- [ ] Confirm SSURGO-to-field crosswalk method for NCCPI (direct overlay vs existing lookup).
- [ ] Confirm PRISM `vpdmax` product/resolution path and target year span for this first cut.
- [ ] Confirm final Valu1 column subset to persist from gSSURGO (`corn/soy/all` families) for model + future features.
