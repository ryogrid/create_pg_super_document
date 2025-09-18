# ExecBitmapHeapEstimate

## Location
[src/backend/executor/nodeBitmapHeapscan.c:817-830](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapHeapscan.c#L817-L830)

## Overview
This function estimates the amount of shared memory space needed for a parallel bitmap heap scan operation.

## Definition
```c
void ExecBitmapHeapEstimate(BitmapHeapScanState *node, ParallelContext *pcxt)
```

## Detailed Description
ExecBitmapHeapEstimate is part of PostgreSQL's parallel query execution framework. It calculates the shared memory requirements for bitmap heap scan operations that will run in parallel across multiple worker processes. The function updates the parallel context's estimator with the memory requirements needed for coordinating parallel bitmap heap scans.

The function registers two specific requirements:
1. Space for one ParallelBitmapHeapState structure to hold shared state
2. One shared memory TOC (Table of Contents) key for identifying the shared state

This estimation is performed during the parallel query planning phase, before worker processes are actually spawned.

## Parameters / Member Variables
- `node`: Pointer to BitmapHeapScanState containing the execution state for this bitmap heap scan node
- `pcxt`: Pointer to ParallelContext structure that manages parallel execution coordination and shared memory allocation

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_estimate_chunk
  - shm_toc_estimate_keys
  - [BitmapHeapScanState](../B/BitmapHeapScanState.md) (structure)
  - [ParallelContext](../P/ParallelContext.md) (structure)
  - [ParallelBitmapHeapState](../P/ParallelBitmapHeapState.md) (structure)
- Called from (representative examples):
  - ExecParallelEstimate

## Notes and Other Information
This function is called during parallel query setup, before any worker processes are created. The estimated memory is later allocated by ExecBitmapHeapInitializeDSM. The function follows PostgreSQL's standard pattern for parallel node estimation where each node type estimates its own shared memory needs.