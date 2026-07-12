# Field-Year-Crop State

| Slice | Status | Notes |
|---|---|---|
| OS-001 | implemented | layout and product contract baseline created |
| OS-002 | implemented | Python script contracts and unit fixtures |
| OS-003 | implemented | local synthetic GORC workflow |
| OS-004 | verified | local real input metadata workflow passed with repo-local h18v07 staging; CDL ZIP prefetch workaround used pending generic archive extraction |
| OS-005 | verified | real local field-crop-year workflow passed for h18v07 2010 with Numpy/GDAL pair counts |
| OS-006 | verified | fake HPCC synthetic graduation passed with generated inline workflow and fake Slurm workers |
| OS-007 | verified | external HTTPS controller preflight passed; HPCC h18v07 2010 one-tile run completed with validation status passed |
| OS-008 | implemented | production packaging scripts, delivery manifest, Google Drive publish plan, and tiny dry run added |
| OS-009 | verified | production pilot `run-dcbf2b84ffb9fc1b49abdeec34960188` completed on Google VM/HPCC for 2010 x h18v07,h23v08 with 9/9 work items completed, delivery validation passed, 631857 merged counts/summary rows, 18 planned gdrive publish objects, and caretaker evidence of two live worker sessions; publication pilot `run-b0be8e51b1a7ea6e82a0f55f97738fbd` completed 10/10 work items and published `tile-field-year-crop-delivery.zip` to `gdrive:Data/ETL/tile-field-year-crop/os009-gdrive-003/` |
