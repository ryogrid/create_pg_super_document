# ExecIndexOnlyScanReInitializeDSM

## Location
src/backend/executor/nodeIndexonlyscan.c: 764 - 776

## Overview
Resets the shared state of a parallel index-only scan before beginning a fresh scan, ensuring all parallel workers start from a clean state.

## Definition
```c
void ExecIndexOnlyScanReInitializeDSM(IndexOnlyScanState *node, ParallelContext *pcxt)
```

## Detailed Description
This function is a lightweight wrapper that resets the parallel index-only scan state to allow for a fresh scan to begin. It is typically called when a parallel query needs to restart scanning from the beginning, such as in nested loop joins or when rescanning is required. The function delegates the actual reset operation to the index access method's parallel rescan functionality.

The function serves as the executor-level interface for reinitializing parallel index-only scans, ensuring that the shared scan state is properly reset across all participating worker processes before beginning a new scan iteration.

## Parameters / Member Variables
- `node`: IndexOnlyScanState containing the executor state for the index-only scan node, particularly the scan descriptor that needs to be reset
- `pcxt`: ParallelContext providing the parallel execution context (currently unused but maintained for interface consistency)

## Dependencies
- Functions called/Symbols referenced:
  - [index_parallelrescan](../i/index_parallelrescan.md)
- Types used:
  - [IndexOnlyScanState](../I/IndexOnlyScanState.md)
  - [ParallelContext](../P/ParallelContext.md)
- Called from (representative examples):
  - [ExecParallelReInitializeDSM](ExecParallelReInitializeDSM.md)

## Notes and Other Information
- This is a very simple function that primarily serves as an interface wrapper around index_parallelrescan
- The ParallelContext parameter is not currently used in the implementation but is maintained for API consistency with other DSM functions
- This function is part of the parallel query execution framework and is called during query plan reexecution scenarios
- The reset operation ensures that all parallel workers will restart scanning from the beginning of the index
- Unlike ExecIndexOnlyScanInitializeDSM, this function assumes the parallel infrastructure is already set up and only needs to be reset