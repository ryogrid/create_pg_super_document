# ExecIndexOnlyScanInitializeWorker

## Location
src/backend/executor/nodeIndexonlyscan.c: 777 - 799

## Overview
Initializes a worker process for participation in a parallel index-only scan by retrieving the shared parallel scan descriptor and setting up the local scan state.

## Definition
```c
void ExecIndexOnlyScanInitializeWorker(IndexOnlyScanState *node, ParallelWorkerContext *pwcxt)
```

## Detailed Description
This function is called by parallel worker processes to initialize their local state for participating in a parallel index-only scan. It retrieves the previously initialized parallel index scan descriptor from shared memory and uses it to create a local index scan descriptor. The function mirrors some of the setup performed in ExecIndexOnlyScanInitializeDSM but from the worker's perspective, focusing on joining an existing parallel scan rather than creating a new one.

The function performs these key operations:
1. Looks up the shared parallel index scan descriptor using the plan node ID
2. Creates a parallel-capable index scan descriptor for this worker
3. Configures the scan for index-only access by setting xs_want_itup
4. Optionally starts the scan if runtime keys are available

This function is essential for enabling multiple worker processes to collaboratively scan an index, with each worker processing different portions of the index data.

## Parameters / Member Variables
- `node`: IndexOnlyScanState containing the executor state for the index-only scan node in this worker process, including relation descriptors and scan configuration
- `pwcxt`: ParallelWorkerContext providing access to the shared memory table of contents and other parallel worker infrastructure

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_lookup
  - index_beginscan_parallel
  - index_rescan
- Types used:
  - IndexOnlyScanState
  - ParallelWorkerContext
  - ParallelIndexScanDesc
- Called from (representative examples):
  - ExecParallelInitializeWorker

## Notes and Other Information
- This function is the worker-side counterpart to ExecIndexOnlyScanInitializeDSM
- The shared parallel scan descriptor is looked up using the plan node ID as the key
- Each worker gets its own local IndexScanDesc but shares the underlying parallel scan coordination
- The xs_want_itup setting ensures that index tuples are returned rather than heap tuples, which is the defining characteristic of index-only scans
- Runtime key handling is consistent with the leader process - scans only start immediately if keys are ready
- Workers do not initialize the visibility map buffer (ioss_VMBuffer) in this function; it will be managed separately during scan execution