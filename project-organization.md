# Project Organization

This document defines the expected layout for future project repos and
for AI-generated Strategic Concepts and Operational Slices.

## Core Rule

Organize by product and workflow behavior first. Treat runtime placement as a
configuration concern, not as part of the project or workflow identity.

## Top Level

Use a small, stable top level:

- one project file at the repo root
- one product root directory per publishable dataset or closely related product
- one `common/` directory for shared repo-wide assets and helper scripts
- one `legacy/` directory for retired or moved assets
- one organization document that explains the repo contract

Avoid scattering product logic across top-level `assets/`, `db/`, `queries/`,
`pipelines/`, or `scripts/` directories once a product root exists.

## Common Root

Use `common/` for material that is shared across multiple products or workflows
and does not belong to a single product root.

- `common/scripts/` for reusable helper scripts
- `common/assets/` for reusable static inputs, templates, or fixtures

Keep `common/` generic and cross-cutting. If an item belongs to only one
product, keep it under that product root instead.

## Project File

The project file describes the repository-level identity and shared metadata.

- keep it at the repo root
- keep one project file per repo unless a deliberate multi-project boundary is
  being designed
- do not encode local, fake, or real runtime selection in the project file

## Product Root

Each product should live in its own directory, for example `field-year-crop/`.
That directory owns the active implementation for that product:

- `README.md` for the product overview
- `GORC_VERSION.md` or similar version pinning notes
- `workflow.json` as the canonical workflow definition for the product
- `configs/` for controller and worker config files
- `scripts/` for Python helpers, shell wrappers, and smoke scripts
- `submissions/` for submission templates
- `tests/` for fixtures and executable contracts
- `docs/` for Strategic Concepts, Operational Slices, runbooks, and status
  notes

## Workflow Model

Prefer one canonical workflow per publishable dataset unless a strong product
reason requires more than one.

- the workflow name should describe the dataset or product outcome
- the workflow file should live inside the product root as `workflow.json`
- workflow files should be runtime-agnostic
- workflow definitions should not embed local vs fake HPCC vs real HPCC
  branching
- if multiple runtimes are needed, put the differences in config files

## Runtime Model

Runtime selection belongs in config files and launch wrappers.

- local
- fake HPCC
- real HPCC

Treat these as controller and worker configuration modes. They should not change
the product identity, the project file identity, or the workflow identity.

## SC and OS Writing Guidance

When writing Strategic Concept or Operational Slice specs, follow this layout
rule set:

- describe the product root and its responsibilities explicitly
- identify the canonical workflow and the dataset it produces
- separate runtime configs from workflow logic
- put platform-specific variation into `configs/`, not into the project file
- use `legacy/` for retired assets instead of keeping old top-level folders
- keep documentation paths aligned with the active product root

## Transition Note

During a repo migration, temporary placeholder directories are acceptable, but
the intended end state is a single project file, a single product root per
dataset, and runtime-specific config files rather than runtime-specific project
or workflow identities.
