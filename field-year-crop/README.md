# Field Year Crop

This folder contains the LandCore field-year-crop implementation root.

It owns:

- `land-core.project.json`
- `project-organization.md`
- project configuration
- `workflow.json` as the canonical workflow definition
- submission templates
- runtime controller and worker configuration
- LandCore-specific scripts
- validation tests and fixtures
- implementation docs and runbooks
- GORC version pinning

The design and slice planning for this product live in:

- `field-year-crop/docs/`

Runtime mode is controlled by config only:

- local
- fake HPCC
- real HPCC

GORC itself is a separate reusable orchestration runtime maintained outside
this repository.



