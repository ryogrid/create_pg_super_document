# ExecMemoizeRetrieveInstrumentation

## Location
[src/backend/executor/nodeMemoize.c:1249-1262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMemoize.c#L1249-L1262)

## Overview
Transfers memoize execution statistics from shared memory to private memory, consolidating instrumentation data collected from parallel worker processes.

## Definition
```c
void ExecMemoizeRetrieveInstrumentation(MemoizeState *node)
```

## Detailed Description
ExecMemoizeRetrieveInstrumentation is responsible for copying instrumentation data from the Dynamic Shared Memory (DSM) segment to private memory in the leader process. This function is called after parallel execution completes to consolidate statistics collected from all worker processes. The function allocates private memory, copies the entire SharedMemoizeInfo structure including all worker instrumentation data, and updates the node's shared_info pointer to reference the private copy.

This transfer is necessary because the shared memory segment may be deallocated when the parallel context ends, so the statistics need to be preserved in private memory for later access (e.g., for EXPLAIN ANALYZE output or other instrumentation reporting).

## Parameters / Member Variables
- `node`: Pointer to the MemoizeState structure containing the reference to shared memory statistics

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (allocates private memory from PostgreSQL's memory context)
  - memcpy (copies memory contents from shared to private memory)
- Types referenced:
  - [MemoizeState](../M/MemoizeState.md) (memoize execution state structure)
  - [SharedMemoizeInfo](../S/SharedMemoizeInfo.md) (shared memory structure containing consolidated statistics)
  - [MemoizeInstrumentation](../M/MemoizeInstrumentation.md) (individual worker instrumentation data)
- Called from:
  - [ExecParallelRetrieveInstrumentation](ExecParallelRetrieveInstrumentation.md) (main parallel instrumentation retrieval function)

## Notes and Other Information
- Only performs the transfer if shared_info is not NULL (i.e., parallel execution with instrumentation was used)
- Calculates the exact size needed including the header and all worker instrumentation slots
- After the transfer, node->shared_info points to private memory instead of shared memory
- The copied data includes statistics from all workers, allowing for comprehensive performance analysis
- This is typically one of the last operations in parallel query execution for memoize nodes
- The private copy persists beyond the lifetime of the parallel context, enabling post-execution analysis

## Simplified Source

```c
void ExecMemoizeRetrieveInstrumentation(MemoizeState *node) {
    // Skip if no shared instrumentation data
    if (node->shared_info == NULL)
        return;

    // Calculate total size for SharedMemoizeInfo + all worker instrumentation
    Size size = offsetof(SharedMemoizeInfo, sinstrument) +
                node->shared_info->num_workers * sizeof(MemoizeInstrumentation);

    // Copy shared memory statistics to private memory for persistence
    SharedMemoizeInfo *private_copy = palloc(size);
    memcpy(private_copy, node->shared_info, size);
    node->shared_info = private_copy;
}
```