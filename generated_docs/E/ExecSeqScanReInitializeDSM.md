# ExecSeqScanReInitializeDSM

## Location
src/backend/executor/nodeSeqscan.c: 278 - 293

## Overview
ExecSeqScanReInitializeDSM resets the shared memory state for parallel sequential scans before beginning a fresh scan operation.

## Definition
```c
void ExecSeqScanReInitializeDSM(SeqScanState *node, ParallelContext *pcxt)
```

## Detailed Description
This function reinitializes the Dynamic Shared Memory (DSM) structures used for parallel sequential scans when a rescan operation is needed. It retrieves the existing parallel scan descriptor from the current scan descriptor and calls table_parallelscan_reinitialize() to reset the shared scan state, allowing the parallel scan to restart from the beginning while maintaining the same shared memory structures.

## Parameters / Member Variables
- `node`: A pointer to the SeqScanState structure containing the sequential scan state information
- `pcxt`: A pointer to the ParallelContext structure that manages parallel execution context (unused in current implementation but required for interface consistency)

## Dependencies
- Functions called/Symbols referenced:
  - table_parallelscan_reinitialize
- Types referenced:
  - SeqScanState
  - ParallelContext
  - ParallelTableScanDesc
- Called from (representative examples):
  - ExecParallelReInitializeDSM (in execParallel.c)

## Notes and Other Information
- This function is part of PostgreSQL's parallel query execution infrastructure
- It is called during rescan operations in parallel execution contexts
- The function assumes that a parallel scan descriptor already exists (set up by ExecSeqScanInitializeDSM)
- The ParallelContext parameter is currently unused but maintained for interface consistency
- This is more efficient than completely destroying and recreating the parallel scan structures
- Located in src/backend/executor/nodeSeqscan.c at lines 278-293