# OS-007: Real HPCC One-Tile Preflight

Status: Verified
Scope: LandCore repository only
Do not modify: GORC core, GORC worker, GORC controller, GORC data-asset providers, GORC geospatial plugins

Recommended model: 5.4
Recommended reasoning: high


## Purpose

Prepare and run the first real HPCC one-tile LandCore field-crop-year workflow.

This slice is primarily configuration, runbook, and preflight. It should not expand to full production.

## Allowed Files

```text
field-year-crop/configs/hpcc-controller.template.json
field-year-crop/configs/hpcc-worker.template.json
field-year-crop/configs/hpcc-worker-gdrive.template.json
field-year-crop/containers/goetl-worker-gdal-os007/Dockerfile
field-year-crop/submissions/hpcc-field-crop-year-2010.template.json
field-year-crop/scripts/smoke/hpcc_preflight.sh
field-year-crop/scripts/smoke/hpcc_one_tile_2010.sh
field-year-crop/docs/hpcc-runbook.md
field-year-crop/docs/runbook.md
field-year-crop/docs/STATE.md
field-year-crop/docs/issues.md
```

## Precondition

OS-006 fake HPCC must pass.

If fake HPCC has not passed, do not run real HPCC.

## Current HPCC Target Assumptions

Use this target for the first real HPCC attempt:

```text
ssh host alias = dev-amd20.passwordless
base operations root = /mnt/scratch/weave151/etl
staged Yan/Roy archive = /mnt/scratch/weave151/data/ReleaseData.7z
extracted Yan/Roy root = /mnt/scratch/weave151/data
year = 2010
tile = h18v07
scheduler = Slurm through sbatch
worker runtime = Singularity/Apptainer
controller placement = external Google VM over HTTPS 443
publish target = HPCC scratch filesystem only
Google Drive = fast follower, not part of OS-007
```

The HPCC home directory is small. Do not use `~` for runtime data, cache,
downloads, extracted rasters, aligned rasters, worker artifacts, controller
staging artifacts, or publish output. Use `/mnt/scratch/weave151/etl` as the
base of HPCC-side operations and keep committed config files templated. With the
external Google VM design, the controller ledger belongs on the VM persistent
disk, not HPCC scratch.

Suggested scratch layout:

```text
/mnt/scratch/weave151/etl/runtime
/mnt/scratch/weave151/etl/runtime/data
/mnt/scratch/weave151/etl/runtime/tmp
/mnt/scratch/weave151/etl/runtime/logs
/mnt/scratch/weave151/etl/cache
/mnt/scratch/weave151/etl/source
/mnt/scratch/weave151/etl/publish
/mnt/scratch/weave151/etl/scripts
/mnt/scratch/weave151/etl/images
```

The `/mnt/scratch/weave151` filesystem is shared by login and compute nodes and
is cleaned after roughly 45 days. OS-007 output should be treated as
reproducible preflight output, not durable publication.

Live inspection on `dev-amd20.passwordless` showed:

```text
hostname = dev-amd20
user = weave151
sbatch = /usr/bin/sbatch, slurm 25.05.5
singularity = /usr/bin/singularity, singularity-ce version 4.1.2-jammy
7z = /usr/bin/7z
/mnt/scratch/weave151/data tile directories = 361
/mnt/scratch/weave151/data/ReleaseData.7z = present
/mnt/scratch/weave151/data/h18v07/WELD_h18v07_2010_field_segments = present
companion /mnt/scratch/weave151/data/h18v07/WELD_h18v07_2010_field_segments.hdr = present
/mnt/scratch/weave151/data/h23v08/WELD_h23v08_2010_field_segments.hdr = present
```

## Controller Placement

Workers do not read or write the controller SQLite database directly. Worker
processes communicate only with the controller HTTP API. Therefore,
`main_database_connection_string` belongs to the machine where the controller
process runs:

```text
controller on Google VM = VM persistent disk database path
temporary laptop controller = local laptop/WSL database path, only for local debugging
```

The worker-visible paths that must be on `/mnt/scratch/weave151/etl` are the
worker runtime root, worker data/cache/tmp/log roots, staged/downloaded data,
generated Slurm scripts, images, and publish output.

The intended OS-007 controller placement is an external Google VM with a stable
HTTPS endpoint on port 443. Slurm workers should call the controller through a
worker-visible URL such as:

```text
https://<CONTROLLER_HOSTNAME>/
```

The Google VM controller should use persistent disk storage for its SQLite
ledger and logs. Do not put the controller ledger on HPCC scratch when the
controller is external.

The Google VM must also be able to submit and manage HPCC work through GORC SSH
transport:

```text
Google VM controller -> hpcc.msu.edu gateway -> dev-amd20 -> Slurm sbatch
Slurm worker -> https://<CONTROLLER_HOSTNAME>/ over port 443
```

GORC now has explicit SSH jump-host support and controller callback tunnel
support. For OS-007, use `jump_hosts` for the Google VM to reach `dev-amd20`
through `hpcc.msu.edu`. Do not require reverse SSH tunneling for worker
callbacks.

Rejected controller placements for OS-007:

- HPCC gateway controller: do not run controller or worker payloads on the
  gateway.
- HPCC dev-node controller: dev nodes have an execution time limit and are not a
  durable controller host.
- Laptop reverse-tunnel controller: acceptable only as temporary local
  experimentation. Live probing showed SSH reverse forwards were reachable on
  the remote host loopback address but not as a worker-visible
  `hpcc.msu.edu:<port>` or `dev-amd20:<port>` URL. Treat this path as too
  fragile for the real preflight.

The controller URL cannot be `http://localhost:8080` from the worker
perspective unless the worker process is running on the same host as the
controller.

## SSH Host-Key Policy

GORC's SSH transport supports direct SSH targets and explicit `jump_hosts`.
Rendered OS-007 config should model the HPCC path explicitly rather than relying
on the local OpenSSH `ProxyJump` alias:

```json
{
  "host": "dev-amd20",
  "port": "22",
  "user": "weave151",
  "identity_file": "<CONTROLLER_VM_HPCC_PRIVATE_KEY_PATH>",
  "host_key_policy": "pinned",
  "pinned_host_key": "<DEV_AMD20_PUBLIC_HOST_KEY>",
  "jump_hosts": [
    {
      "host": "hpcc.msu.edu",
      "port": "22",
      "user": "weave151",
      "identity_file": "<CONTROLLER_VM_HPCC_PRIVATE_KEY_PATH>",
      "host_key_policy": "pinned",
      "pinned_host_key": "<HPCC_GATEWAY_PUBLIC_HOST_KEY>"
    }
  ]
}
```

Prefer `pinned` for OS-007 if the host keys can be collected safely. The
rendered Google VM config may alternatively use `known_hosts` with an explicit
`known_hosts_file` if that file is provisioned on the VM. Do not use
`insecure_ignore` for the real preflight except as a documented local-only
debugging step.

For local inspection, the workstation aliases are:

```text
gateway alias = hpcc.msu.edu.passwordless
dev alias = dev-amd20.passwordless
```

Do not commit a private SSH key or any credential. In rendered GORC config,
store only public host-key material, for example `ssh-rsa AAA...`; do not
include the hostname prefix in `pinned_host_key`.

## Required Preflight Checks

`hpcc_preflight.sh` must check and print:

```text
controller public HTTPS URL
controller /status reachability from the local or VM side
hostname
current user
working directory
available modules or singularity/apptainer version
Slurm sbatch availability
scratch/project path existence
controller URL reachability from the HPCC dev-node side
controller URL reachability from a Slurm compute job
free disk space for cache/data/publish roots
GORC worker executable or Singularity image path
GDAL availability inside the worker image
Python GDAL import availability inside the worker image
Numpy availability inside the worker image
7z availability for optional staged Yan/Roy ReleaseData.7z extraction
network or download path availability for CDL 2010 acquisition
ability to read /mnt/scratch/weave151/data/h18v07/WELD_h18v07_2010_field_segments with its companion .hdr
ability to write publish root
```

Do not require rclone or `GOET_GDRIVE_TOKEN` in OS-007. Google Drive checks
belong to OS-009 unless a later local rendered config explicitly opts into them.

## Runtime Requirements

The HPCC config should use one tile/year only:

```text
year = 2010
yanroy raster = /mnt/scratch/weave151/data/h18v07/WELD_h18v07_2010_field_segments
yanroy source archive = /mnt/scratch/weave151/data/ReleaseData.7z
CDL source = download on HPCC during the run
```

Do not add production fanout yet.

The Slurm submission must make resource fields explicit, even if the values are
cluster-standard and conservative for the first run. Keep them templated rather
than tuned for production:

```text
<SLURM_PARTITION>
<SLURM_ACCOUNT>
<SLURM_TIME>
<SLURM_MEM>
<SLURM_CPUS_PER_TASK>
```

Do not add production resource tuning in OS-007.

The worker should run through Singularity/Apptainer. For the verified OS-007
run, the LandCore-specific worker image was built from
`field-year-crop/containers/goetl-worker-gdal-os007/Dockerfile` and staged at:

```text
/mnt/scratch/weave151/etl/runtime/images/goetl-worker-gdal-os007.sif
```

The image must include at least:

```text
goetl-worker
goet-geospatial on PATH
python3
GDAL command-line tools
Python osgeo.gdal bindings
numpy
7z/7za/7zr or an equivalent extraction path for ReleaseData.7z
```

The preflight must fail clearly if these are missing.

## Verified Run

Verified on 2026-07-10 using:

```text
controller URL = https://34-10-225-164.sslip.io
worker image = /mnt/scratch/weave151/etl/runtime/images/goetl-worker-gdal-os007.sif
Yan/Roy raster = /mnt/scratch/weave151/data/h18v07/WELD_h18v07_2010_field_segments
publish root = /mnt/scratch/weave151/etl/publish/field-crop-year/h18v07/2010
submission = run-fe4b978addfb854ad644b2c2ba899dca
```

Result:

```text
Status: completed
Known work items: 4
Completed: 4
Failed: 0
validation status: passed
distinct fields: 47922
counts rows: 221084
summary rows: 221084
total counted pixels: 20902248
```

## Data Handling

Do not commit real data paths if they reveal private storage layout. Use templates with placeholders:

```text
<HPCC_SCRATCH_ROOT>
<LANDCORE_DATA_ROOT>
<GORC_GDAL_IMAGE>
<CONTROLLER_PUBLIC_URL>
<CONTROLLER_VM_HPCC_PRIVATE_KEY_PATH>
<DEV_AMD20_PUBLIC_HOST_KEY>
<HPCC_GATEWAY_PUBLIC_HOST_KEY>
<YANROY_H18V07_PATH>
<PUBLISH_ROOT>
<SLURM_PARTITION>
<SLURM_ACCOUNT>
<SLURM_TIME>
<SLURM_MEM>
<SLURM_CPUS_PER_TASK>
```

A local ignored config may be created by the user later, but this OS should commit only templates and documentation.

For the current target, render the placeholders locally as:

```text
<HPCC_SCRATCH_ROOT> = /mnt/scratch/weave151/etl
<LANDCORE_DATA_ROOT> = /mnt/scratch/weave151/data
<YANROY_RELEASE_ARCHIVE> = /mnt/scratch/weave151/data/ReleaseData.7z
<YANROY_H18V07_PATH> = /mnt/scratch/weave151/data/h18v07/WELD_h18v07_2010_field_segments
<PUBLISH_ROOT> = /mnt/scratch/weave151/etl/publish
<CONTROLLER_PUBLIC_URL> = https://<google-vm-controller-hostname>/
```

Download CDL 2010 on HPCC into the scratch operations root. Do not rely on the
small HPCC home directory for CDL downloads or extraction.

## Secret Handling

Google Drive is intentionally out of OS-007. Do not require rclone,
`GOET_GDRIVE_TOKEN`, or any other Google credential for the first real HPCC
preflight.

The external controller endpoint must not expose an unauthenticated public GORC
API for longer than a controlled preflight window. Prefer a firewall allowlist
for known HPCC egress addresses if they can be identified. If allowlisting is
not available yet, use a short-lived controller VM and document the exposure
window. Do not commit controller VM private keys, cloud credentials, TLS private
keys, or rendered firewall credentials.

If a later HPCC slice uses Google Drive or any other credentialed input, the
submission/workflow must use a GORC sensitive protected reference such as:

```json
{
  "name": "gdrive_token",
  "type": "string",
  "sensitive": true,
  "protected_ref": {
    "provider": "worker_env",
    "key": "GOET_GDRIVE_TOKEN"
  }
}
```

A later credentialed preflight may verify that `GOET_GDRIVE_TOKEN` exists in the
worker environment, but it must not echo, hash, truncate, or write the value.
Plaintext credentials, rendered rclone config, OAuth tokens, refresh tokens, and
service-account JSON are out of scope for committed templates.

## Validation Command

```bash
bash field-year-crop/scripts/smoke/hpcc_preflight.sh
bash field-year-crop/scripts/smoke/hpcc_one_tile_2010.sh
```

The second command may be documented as manual if direct HPCC access is not available to the implementing model.

## Stop Conditions

Stop and record an issue if:

- HPCC lacks compatible Singularity/Apptainer;
- GDAL worker image cannot run;
- input paths are inaccessible from compute nodes;
- Google VM controller cannot reach HPCC through the gateway and dev node;
- HPCC dev node or Slurm compute job cannot reach the controller HTTPS URL;
- publication root is not writable;
- raster values exceed current plugin dtype constraints.

## Completion Criteria

- HPCC preflight script exists and passes against the external HTTPS controller.
- HPCC one-tile runbook exists.
- Template configs are present.
- LandCore GDAL worker image Dockerfile exists and the SIF is staged on HPCC.
- One-tile command is documented and has completed once on HPCC.
- No private credentials or sensitive token contents are committed.
