# ExecHashRetrieveInstrumentation

## Location
src/backend/executor/nodeHash.c: 2826 - 2856

## Overview
ExecHashRetrieveInstrumentation copies hash table instrumentation data from shared memory to backend-local memory before DSM detachment, preserving worker statistics for EXPLAIN output.

## Definition
```c
void ExecHashRetrieveInstrumentation(HashState *node)
```

## Detailed Description
This function plays a critical role in preserving instrumentation data collected from parallel workers during hash table operations. When parallel query execution completes, the Dynamic Shared Memory (DSM) segment containing worker statistics will be detached and become inaccessible. This function must be called before that happens to ensure the data remains available for EXPLAIN ANALYZE output.

The function performs the following operations:
1. Checks if shared instrumentation data exists
2. Calculates the total size needed for the shared data structure and all worker instrumentation records
3. Allocates backend-local memory to hold a copy of the data
4. Copies the entire shared instrumentation area to the local memory

After this function completes, the shared_info pointer in the HashState will point to backend-local memory instead of shared memory, allowing EXPLAIN processing to access the worker statistics even after DSM detachment.

## Parameters / Member Variables
- `node`: HashState pointer containing the reference to shared instrumentation data that will be replaced with a local copy

## Dependencies
- Functions called/Symbols referenced:
  - palloc
  - memcpy
  - offsetof (macro)
- Types used:
  - HashState
  - SharedHashInfo
  - HashInstrumentation
- Called from (representative examples):
  - ExecParallelRetrieveInstrumentation

## Notes and Other Information
- The function returns early if no shared instrumentation data exists (shared_info is NULL)
- The size calculation accounts for both the SharedHashInfo header and the variable-length array of HashInstrumentation structures for all workers
- After copying, the original shared memory reference is lost, but this is intentional since the DSM segment will be detached
- This function is essential for accurate EXPLAIN ANALYZE output in parallel hash joins, as it preserves worker-collected statistics
- The copied data includes instrumentation from all workers, allowing for aggregate statistics calculation
- This function is part of the parallel query cleanup sequence and must be called before DSM detachment