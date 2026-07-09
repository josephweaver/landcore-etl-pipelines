# Model Implementation Plan

Goal: use the least expensive model likely to complete each OS without creating extra churn.

General rule:

```text
Use 5.3-Codex-Spark only for very mechanical docs/scaffolding.
Use 5.4-mini for most LandCore repo implementation.
Use 5.4 for environment/runtime slices where failed smoke-test debugging is likely.
Avoid Spark for smoke-test-heavy work unless the slice is purely mechanical.
Reserve 5.5 for review, blocker diagnosis, or redesign.
```

| OS | Recommended model | Reasoning level | Why |
|---|---:|---:|---|
| OS-001 repo layout and product contract | 5.3-Codex-Spark | low | Mostly docs and folders. No runtime debugging. |
| OS-002 Python script contracts and unit fixtures | 5.4-mini | medium | Standard-library Python, deterministic CSV tests, modest logic. |
| OS-003 local synthetic GORC workflow | 5.4-mini | medium-high | Needs GORC JSON shape and smoke script; avoid Spark if smoke gets flaky. |
| OS-004 local real-input materialization | 5.4-mini | high | Needs data asset config, max asset size, CDL archive discovery, Yan/Roy sidecar checks. |
| OS-005 local real field-crop-year product | 5.4-mini | high | Core product integration, alignment, pair counts, validation. |
| OS-006 fake HPCC graduation | 5.4 | medium | Runtime/environment failures are likely; not ideal for Spark. |
| OS-007 real HPCC one-tile preflight | 5.4 | high | External environment and Singularity/Slurm path issues. |
| OS-008 production tiling and delivery | 5.4-mini | medium | Mostly workflow expansion, merge scripts, docs, delivery packaging. Use 5.4 if fanout/resource constraints get complicated. |
| OS-009 Google Drive source and publication connector trial | 5.4 | high | Real rclone, worker-local credentials, Shared Drive source access, Drive folder publication, and redaction checks are external-runtime heavy. |
| Cross-slice review | 5.5 | medium or high | Use only after 2-3 OS slices or when architecture ambiguity appears. |

## Suggested Cadence

```text
Run OS-001 through OS-003 first.
Stop.
Review outputs manually.

Then run OS-004.
Stop.
Confirm /tmp/h18v07.hdr opens and CDL archive member discovery works.

Then run OS-005.
Stop.
Inspect field/crop counts and summaries.

Only then move to fake HPCC and real HPCC.

Treat OS-009 as a late, optional connector proof. Do not let Google Drive setup
block OS-004 through OS-008; use `local_file` or `registered_location` for
Yan/Roy input acquisition and a local delivery package until the rclone
credential and publication paths are proven.
```

## Smoke-Test Caution

Spark can be useful because it may draw from a separate token bucket, but smoke-test debugging has historically been expensive on Spark. For this work, use Spark only when the expected output is a small file diff, not a runtime integration.
