# LandCore ETL Pipelines

This repository contains LandCore ETL pipelines, legacy pipeline/script assets,
and a field-specific `field-year-crop/` implementation root for producing
LandCore data products.

The orchestration assets run on GORC, a separate public orchestration runtime
maintained outside this repository.

LandCore runs local debug first, then fake HPCC if configured, then real HPCC.

## Layout

- `land-core.project.json`
- `project-organization.md`
- `docs/`
- `field-year-crop/`
- `legacy/`

The old top-level `assets/`, `db/`, `pipelines/`, `queries/`, and `scripts/`
directories were moved under `legacy/`.

## Orchestration

The field-year-crop implementation root owns:

- LandCore project configuration
- LandCore workflow definitions
- LandCore-specific R and Python scripts
- data-product documentation
- runbooks and validation scripts
- runtime version pinning

GORC itself is not vendored into this repository. Install GORC from:

https://github.com/josephweaver/go-etl

Use the pinned GORC version or commit listed in `GORC_VERSION.md`.

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

## Working with Codex

This repo is intended to be developed together with the sibling ETL framework
repo:

- `../etl`

If you want Codex to help create or repair pipelines, start the session from the
`research-etl` repo root so it can see the AI routing and prompt-engineering
files.

Recommended workflow:

1. Open Codex in:
   - `../etl`
2. Tell Codex which target repo you are working in:
   - `../landcore-etl-pipelines`
3. For new pipeline creation, ask Codex to:
   - read `CODEX.md`
   - use the `ai_prompts/` guidance
   - inspect existing example pipelines in `../landcore-etl-pipelines`
4. If the pipeline creates a new dataset, also have Codex create or update the
   matching entry in:
   - `../landcore-data-catalog`

Useful prompt to start a session:

```text
Read CODEX.md in ../etl, use the ai_prompts guidance for pipeline authoring,
inspect relevant existing pipelines in ../landcore-etl-pipelines, and help me
create or update one pipeline plus its matching data-catalog entry.
```

Useful prompt for status guidance:

```text
Read CODEX.md in ../etl, use the pipeline progress checklist, inspect the current
pipelines in ../landcore-etl-pipelines, and tell me what the next critical-path
step is.
```

Useful prompt for debugging:

```text
Read CODEX.md in ../etl, use the pipeline failure triage checklist, inspect the
failing pipeline and logs, fix the issue, and if the lesson is reusable update
the relevant ai_prompts file too.
```
