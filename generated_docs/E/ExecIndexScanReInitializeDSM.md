# ExecIndexScanReInitializeDSM

## Location
src/backend/executor/nodeIndexscan.c: 1697 - 1709

## Overview
This function resets shared state in a parallel index scan before beginning a fresh scan, ensuring proper coordination between parallel workers.

## Definition


## Detailed Description
 is a parallel execution function that reinitializes the dynamic shared memory (DSM) state for an index scan node in a parallel query execution context. This function is called when a parallel plan needs to restart or reset an index scan operation, ensuring that all shared state between parallel workers is properly reset before beginning a fresh scan. The function serves as a wrapper around the lower-level  function, providing the executor-level interface for parallel index scan reinitialization.

## Parameters / Member Variables
- : Pointer to the IndexScanState structure containing the current state of the index scan operation
- : Pointer to the ParallelContext structure containing information about the parallel execution context (parameter present but not directly used in the function body)

## Dependencies
- Functions called/Symbols referenced:
  - [index_parallelrescan](../i/index_parallelrescan.md)
- Called from (representative examples):
  - [ExecParallelReInitializeDSM](ExecParallelReInitializeDSM.md)

## Notes and Other Information
- This function is part of PostgreSQL's parallel query execution framework
- The function is very simple, serving primarily as an interface layer between the executor and the index access method
- The ParallelContext parameter is included for consistency with the parallel execution interface but is not used within this specific function
- Located in src/backend/executor/nodeIndexscan.c:1697-1709
- The actual work is delegated to the index access method layer via index_parallelrescan