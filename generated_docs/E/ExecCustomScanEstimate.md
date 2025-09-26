# ExecCustomScanEstimate

## Location
[src/backend/executor/nodeCustom.c:161-173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeCustom.c#L161-L173)

## Overview
Estimates the shared memory requirements for a custom scan node in a parallel query execution context.

## Definition
```c
void ExecCustomScanEstimate(CustomScanState *node, ParallelContext *pcxt)
```

## Detailed Description
ExecCustomScanEstimate is responsible for calculating the shared memory space needed by a custom scan node when executing in parallel mode. This function is called during the parallel query planning phase to determine how much shared memory should be allocated for the custom scan's dynamic shared memory (DSM) segment. If the custom scan provider implements the EstimateDSMCustomScan method, this function calls it to get the estimated memory requirement, then registers this estimate with the parallel context's shared memory table of contents (TOC). The function also reserves one key in the TOC for the custom scan's shared memory segment.

## Parameters / Member Variables
- `node`: A pointer to the CustomScanState structure representing the custom scan node
- `pcxt`: A pointer to the ParallelContext structure containing parallel execution context information

## Dependencies
- Functions called/Symbols referenced:
  - [CustomScanState](../C/CustomScanState.md) (structure type)
  - [ParallelContext](../P/ParallelContext.md) (structure type)
  - [CustomExecMethods](../C/CustomExecMethods.md) (structure type)
  - shm_toc_estimate_chunk (shared memory TOC estimation function)
  - shm_toc_estimate_keys (shared memory TOC key estimation function)
- Called from (representative examples):
  - [ExecParallelEstimate](ExecParallelEstimate.md) (general parallel execution estimator)

## Notes and Other Information
- This function is part of PostgreSQL's parallel query execution framework
- If the custom scan provider does not implement EstimateDSMCustomScan, no shared memory is reserved
- The estimated size is stored in the node's pscan_len field for later use during DSM initialization
- The function reserves exactly one key in the shared memory TOC regardless of the actual memory requirement
- This estimation is crucial for proper shared memory allocation in parallel workers