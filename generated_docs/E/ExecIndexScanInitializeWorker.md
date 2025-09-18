# ExecIndexScanInitializeWorker

## Location
src/backend/executor/nodeIndexscan.c: 1710 - 1731

## Overview
This function initializes a parallel worker process for index scan operations by copying relevant shared information from the table of contents (TOC) and setting up the index scan descriptor.

## Definition
```c
void ExecIndexScanInitializeWorker(IndexScanState *node, ParallelWorkerContext *pwcxt)
```

## Detailed Description
`ExecIndexScanInitializeWorker` is a parallel execution function that initializes a worker process to participate in a parallel index scan operation. The function retrieves the shared parallel index scan descriptor from the dynamic shared memory table of contents using the plan node ID, then initializes the worker's index scan descriptor by calling `index_beginscan_parallel`. After setting up the scan descriptor, it conditionally applies scan keys if runtime keys are either not needed or already prepared, allowing the worker to immediately begin scanning if possible.

## Parameters / Member Variables
- `node`: Pointer to the IndexScanState structure that will be initialized for this worker
- `pwcxt`: Pointer to the ParallelWorkerContext containing shared memory information and the table of contents

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_lookup](../s/shm_toc_lookup.md)
  - [index_beginscan_parallel](../i/index_beginscan_parallel.md)  
  - [index_rescan](../i/index_rescan.md)
- Called from (representative examples):
  - [ExecParallelInitializeWorker](ExecParallelInitializeWorker.md)

## Notes and Other Information
- This function is part of PostgreSQL's parallel query execution framework for index scans
- The function handles both immediate scan key application (when runtime keys are ready) and deferred application (when runtime keys need calculation)
- Uses the plan node ID as a key to lookup the shared parallel index scan descriptor in the table of contents
- The parallel index scan descriptor contains shared state needed for coordinating the scan across multiple workers
- Located in src/backend/executor/nodeIndexscan.c:1710-1731
- Runtime keys may need to be calculated based on parameter values, which is why the function checks `iss_RuntimeKeysReady` before applying scan keys