# ExecSeqScanInitializeDSM

## Location
src/backend/executor/nodeSeqscan.c: 256 - 277

## Overview
ExecSeqScanInitializeDSM sets up a parallel heap scan descriptor in shared memory for parallel sequential scan operations.

## Definition
```c
void ExecSeqScanInitializeDSM(SeqScanState *node, ParallelContext *pcxt)
```

## Detailed Description
This function initializes the Dynamic Shared Memory (DSM) structures required for parallel sequential scans. It allocates shared memory for the parallel scan descriptor, initializes it with the current relation and snapshot information, and inserts it into the shared memory table-of-contents. Finally, it begins a parallel scan using the initialized descriptor, setting up the scan state for parallel execution.

## Parameters / Member Variables
- `node`: A pointer to the SeqScanState structure containing the sequential scan state information
- `pcxt`: A pointer to the ParallelContext structure that manages parallel execution context and shared memory

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_allocate
  - table_parallelscan_initialize
  - shm_toc_insert
  - table_beginscan_parallel
- Types referenced:
  - SeqScanState
  - ParallelContext
  - EState
  - ParallelTableScanDesc
- Called from (representative examples):
  - ExecParallelInitializeDSM (in execParallel.c)

## Notes and Other Information
- This function is part of PostgreSQL's parallel query execution infrastructure
- It operates during the initialization phase of parallel query execution
- The function uses the pscan_len value that was previously calculated by ExecSeqScanEstimate
- The parallel scan descriptor is identified in the shared memory TOC using the plan node ID
- The function establishes the current scan descriptor for use in parallel execution
- Located in src/backend/executor/nodeSeqscan.c at lines 256-277