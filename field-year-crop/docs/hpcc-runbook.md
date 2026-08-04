# HPCC Runbook

OS-007 targets the first real HPCC one-tile preflight only. Google Drive
publication is intentionally deferred.

## Target

```text
ssh host alias: dev-amd20.passwordless
operations root: /mnt/scratch/weave151/etl
data root: /mnt/scratch/weave151/data
Yan/Roy archive: /mnt/scratch/weave151/data/ReleaseData.7z
Yan/Roy h18v07: /mnt/scratch/weave151/data/h18v07/WELD_h18v07_2010_field_segments
tile/year: h18v07, 2010
runtime: Singularity/Apptainer worker image
scheduler: Slurm sbatch with explicit resource fields
controller: external Google VM or equivalent dedicated controller over HTTPS 443
publish root: /mnt/scratch/weave151/etl/publish
```

Do not use the HPCC home directory for workflow data. Treat scratch output as
reproducible because `/mnt/scratch/weave151` is cleaned periodically.

Observed on `dev-amd20.passwordless`:

```text
sbatch: /usr/bin/sbatch, slurm 25.05.5
singularity: /usr/bin/singularity, singularity-ce version 4.1.2-jammy
7z: /usr/bin/7z
extracted Yan/Roy tile directories: 361
h18v07 2010 header: present
h23v08 2010 header: present
```

## Before Running

1. Confirm SSH from the machine that will run the controller. For local
   inspection from this workstation:

```bash
ssh hpcc.msu.edu.passwordless hostname
ssh dev-amd20.passwordless hostname
```

2. Render the Google VM controller config from
   `field-year-crop/configs/hpcc-controller.template.json`. The rendered config
   should advertise the HTTPS controller URL, enable bearer authentication with
   token files, and use GORC `jump_hosts` for:

```text
Google VM controller -> hpcc.msu.edu gateway -> dev-amd20
```

3. Capture pinned host keys for both the gateway and dev node, or provision a
   `known_hosts_file` on the VM and use `known_hosts` policy. Put only the key
   type and key body in `pinned_host_key`, for example `ssh-rsa AAA...`. Do not
   commit private SSH keys or token files.

4. Confirm Yan/Roy is staged and extracted:

```text
/mnt/scratch/weave151/data/ReleaseData.7z
/mnt/scratch/weave151/data/h18v07/WELD_h18v07_2010_field_segments
```

5. Prepare or locate a Singularity/Apptainer image that contains:

```text
goetl-worker
python3
GDAL command-line tools
Python osgeo.gdal bindings
numpy
7z/7za/7zr
```

For production pilot runs that enable Google Drive input or publication, use the
rclone-capable worker image:

```text
/mnt/scratch/weave151/etl/runtime/images/goetl-worker-gdal-os007-rclone.sif
```

It contains the OS-007 GDAL/Numpy/7z runtime plus `/usr/bin/rclone`.

6. Set explicit Slurm resource values for preflight and documentation:

```text
partition
account
time
memory
cpus_per_task
```

7. Provision token files without committing token contents:

```text
local client token file for goet submit/status
controller VM client/worker/admin token files
HPCC worker token file readable by Slurm workers
```

8. Confirm the controller URL selected for OS-007 is reachable from a Slurm
   compute job. Use the preflight script below; it checks local HTTPS, HPCC
   dev-node HTTPS, Slurm compute HTTPS, HPCC input paths, token-file presence,
   and worker image dependencies.

## Controller Placement

Workers only talk to the controller API. They do not need database access. Put
`main_database_connection_string` on the same machine as the controller:

```text
Google VM controller: VM persistent disk path
temporary laptop controller: local laptop/WSL path for development only
```

The intended OS-007 placement is a dedicated external controller with public
HTTPS ingress on port 443 and a private loopback controller listener behind the
ingress. Workers should use the public HTTPS `controller_url`; the controller
should use GORC SSH transport with `jump_hosts` to submit Slurm workers on HPCC.

Do not use an HPCC dev-node controller for OS-007. Dev nodes have an execution
time limit and are not durable controller hosts. Laptop reverse-SSH controller
paths are development-only.

Do not run GORC controller or worker payloads on the gateway node. The gateway
may be part of the SSH path, but execution should target the dev node and Slurm
workers.

## Commands

Preflight:

```bash
export CONTROLLER_URL="https://<controller-hostname>"
export CONTROLLER_CLIENT_TOKEN_FILE="<local-client-token-file>"
export GORC_GDAL_IMAGE="/mnt/scratch/weave151/etl/runtime/images/<worker-image>.sif"
export WORKER_CONTROLLER_TOKEN_FILE_HPCC="/mnt/scratch/weave151/etl/runtime/secrets/controller-worker-token"
export CONTAINER_GOET_WORKER_EXECUTABLE="/goetl/goetl-worker"
bash field-year-crop/scripts/smoke/hpcc_preflight.sh
```

One-tile run:

```bash
bash field-year-crop/scripts/smoke/hpcc_one_tile_2010.sh
```

The one-tile script renders a temporary workflow under
`field-year-crop/.run/hpcc-one-tile-2010`, prepares the CDL ZIP/extraction under
HPCC scratch if needed, submits to the configured HTTPS controller with the local
client token file, and polls for completion.

Verified OS-007 run:

```text
submission: run-fe4b978addfb854ad644b2c2ba899dca
status: completed
validation: passed
publish root: /mnt/scratch/weave151/etl/publish/field-crop-year/h18v07/2010
```
