# Issues

## OS-004: GORC ZIP extraction requires explicit member selection

Status: open

The current GORC archive extraction model requires `archive.select` entries and
does not support extracting an unknown ZIP directory generically. OS-004 cannot
represent `HTTP data asset -> ZIP selected_directory extraction -> discover CDL
raster` without first knowing the CDL member path.

Workaround for OS-004: `field-year-crop/scripts/smoke/local_real_input_metadata_2010.sh`
prefetches the CDL ZIP to `/tmp/landcore-field-year-crop/local-real-input-metadata-2010`,
extracts it locally with Python's standard ZIP reader, and submits a worker
metadata workflow that discovers the extracted raster path.

Follow-up: either add a GORC-supported generic archive-directory extraction mode
or keep explicit archive member selection once the CDL member path is known.

## OS-005: Yan/Roy CRS and dtype require LandCore-side compatibility steps

Status: open

The real Yan/Roy h18v07 ENVI raster reports a custom Albers WKT. The current
GORC metadata parser extracts the last `ID["EPSG", ...]` token from WKT, which
can resolve to a projection-parameter identifier rather than a CRS identifier for
this input. OS-005 therefore passes an explicit target CRS WKT, transform,
width, and height to `align_to_grid` instead of relying on `like_raster`.

Yan/Roy rasters are stored as GDAL `Int32`. h18v07 2010 fits within `UInt16`,
but h23v08 contains field IDs up to roughly 74k, so the product path cannot
assume `UInt16` field IDs. OS-005 computes the real field ID min/max first,
requires non-negative IDs, and creates a temporary `UInt32` working copy without
remapping IDs. Pair counting uses the LandCore Numpy worker, preserving Yan/Roy
field IDs and CDL crop-class values directly in `field_id,crop_id,count`.

Follow-up: make GORC CRS extraction distinguish CRS authority codes from
projection parameter codes.

## OS-006: Fast fake Slurm workers can expose a completion race

Status: verified

The fake HPCC synthetic workflow runs very small work items. With fake Slurm in
background mode, the second worker can finish and report completion immediately
after stage activation. One run produced the summary artifacts but the worker's
completion POST returned a transient controller 500, leaving the work item
assigned.

Workaround for OS-006: the smoke generates a temporary wrapper around the
summary script that sleeps briefly before process exit. This preserves product
logic and gives the controller enough time to finish the prior stage transition
before the second worker reports completion.

Follow-up: make the go-etl worker completion client retry transient 500
responses or make controller completion idempotent across this stage transition.

## OS-007: Real HPCC external-controller path requires rendered deployment inputs

Status: verified

The target HPCC environment is reachable through `hpcc.msu.edu` gateway and
`dev-amd20`. Live inspection confirmed Slurm, Singularity, 7z, curl, large
scratch, and extracted Yan/Roy tile directories are available on the dev side.
The controller-placement decision for OS-007 is now an external Google VM or
equivalent dedicated controller exposed over HTTPS 443 with bearer tokens. GORC
uses `jump_hosts` for controller-to-HPCC Slurm submission; workers call the
public HTTPS controller URL.

Remaining operational risk after OS-007: rendered local/VM values are still
operator-provided and must not be committed. These include controller hostname,
token-file paths, SSH private-key paths, host keys, worker image path, and Slurm
resource values.

Verified on 2026-07-10 against the Google VM controller at
`https://34-10-225-164.sslip.io`. `hpcc_preflight.sh` passed from local, HPCC
dev-node, and Slurm compute-node contexts. `hpcc_one_tile_2010.sh` completed
submission `run-fe4b978addfb854ad644b2c2ba899dca` with 4 completed work items
and validation status `passed`.

## OS-007: Slurm resource placeholders may not tune generated worker scripts

Status: resolved

LandCore OS-007 templates and preflight inputs keep partition, account, time,
memory, and CPU values explicit. The go-etl Slurm worker script generator
inspected during OS-007 preparation emits job name, output, and error directives,
but does not appear to emit partition, account, time, memory, or CPU directives
for generated worker jobs yet.

Safe temporary behavior for OS-007: keep the values explicit in rendered
LandCore config, use them for preflight Slurm checks, and verify the one-tile
run on conservative cluster defaults or a go-etl branch that wires these fields
into generated `#SBATCH` directives.

Follow-up: before production scale-out, confirm the active go-etl branch applies
the rendered resource fields to worker Slurm scripts or add that support in
go-etl.

## OS-007: Current HPCC worker image lacks LandCore GDAL runtime dependencies

Status: resolved

Observed preflight state on 2026-07-10:

- controller HTTPS works locally, from `dev-amd20`, and from a Slurm compute job;
- HPCC scratch, Yan/Roy input, worker token file, and staged worker image are
  present;
- staged image: `/mnt/scratch/weave151/etl/runtime/images/goetl-worker.sif`;
- worker executable inside image: `/goetl/goetl-worker`;
- image has `/usr/bin/python3`;
- image does not expose `gdalinfo` on `PATH`;
- direct inspection also showed Python imports for `osgeo.gdal` and `numpy`
  fail in the image.

Why this is blocking: OS-007's LandCore workflow runs Python/GDAL/Numpy
work-items inside the Singularity worker image. The controller/worker transport
is now viable, but the current worker image cannot execute the LandCore raster
steps.

Safe temporary behavior: stop before one-tile submission until a replacement
worker image is staged with `goetl-worker`, `python3`, GDAL command-line tools,
Python `osgeo.gdal`, `numpy`, and `7z`/`7za`/`7zr`.

Resolution: built `field-year-crop/containers/goetl-worker-gdal-os007/Dockerfile`
locally in WSL, converted it to a Singularity SIF, and staged it at
`/mnt/scratch/weave151/etl/runtime/images/goetl-worker-gdal-os007.sif`.
The image exposes `/goetl/goetl-worker` and `goet-geospatial`, plus GDAL,
Python `osgeo.gdal`, `numpy`, and `7z`. `hpcc_preflight.sh` now checks these
dependencies and passes against the staged image.
