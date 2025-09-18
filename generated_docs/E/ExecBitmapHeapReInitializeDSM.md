# ExecBitmapHeapReInitializeDSM

## Location
src/backend/executor/nodeBitmapHeapscan.c: 865 - 893

## Overview
This function resets the shared state of a parallel bitmap heap scan to prepare for a fresh scan execution.

## Definition
```c
void ExecBitmapHeapReInitializeDSM(BitmapHeapScanState *node, ParallelContext *pcxt)
```

## Detailed Description
ExecBitmapHeapReInitializeDSM is responsible for cleaning up and resetting shared memory state when a parallel bitmap heap scan needs to be re-executed. This situation commonly occurs in contexts like nested loop joins where the inner scan may need to be reset and executed multiple times with different outer relation values.

The function performs several cleanup and reset operations:
1. Resets the shared state back to BM_INITIAL to enable leader re-election for the new scan
2. Frees any existing shared TIDBitmap iterator structures from the previous scan execution
3. Resets iterator pointers to invalid values to prevent access to freed memory
4. Maintains the synchronization primitives (spinlock and condition variable) which can be reused

This function ensures that each fresh scan starts with clean state while reusing the existing shared memory allocation and synchronization infrastructure.

## Parameters / Member Variables
- `node`: Pointer to BitmapHeapScanState containing the execution state and reference to shared parallel state
- `pcxt`: Pointer to ParallelContext structure managing the parallel execution environment (currently unused in implementation but maintained for API consistency)

## Dependencies
- Functions called/Symbols referenced:
  - DsaPointerIsValid
  - [tbm_free_shared_area](../t/tbm_free_shared_area.md)
  - [BitmapHeapScanState](../B/BitmapHeapScanState.md) (structure)
  - [ParallelContext](../P/ParallelContext.md) (structure)
  - [ParallelBitmapHeapState](../P/ParallelBitmapHeapState.md) (structure)
  - dsa_area (structure)
  - BM_INITIAL (enum value)
  - InvalidDsaPointer (constant)
- Called from (representative examples):
  - [ExecParallelReInitializeDSM](ExecParallelReInitializeDSM.md)

## Notes and Other Information
This function is called when a parallel plan node needs to be re-executed, such as in nested loops or when rescanning is required. It's important that this function properly frees shared memory resources to prevent memory leaks in long-running queries with many rescan operations. The function preserves the shared memory structure itself and synchronization primitives, only cleaning up scan-specific state. The safety check for DSA availability ensures graceful handling when parallel execution is not active.