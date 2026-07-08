# LandCore Orchestration

This folder contains LandCore-specific orchestration assets for producing LandCore data products.

The workflows in this folder run on GORC, a separate public orchestration runtime maintained outside this repository.

This folder owns:

- LandCore project configuration
- LandCore workflow definitions
- LandCore-specific R and Python scripts
- data-product documentation
- runbooks and validation scripts

GORC itself is not vendored into this repository. Install GORC from:

https://github.com/josephweaver/go-etl

Use the pinned GORC version or commit listed in `GORC_VERSION.md`.