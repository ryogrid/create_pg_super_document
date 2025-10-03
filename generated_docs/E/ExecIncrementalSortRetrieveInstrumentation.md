# ExecIncrementalSortRetrieveInstrumentation

## Location
[src/backend/executor/nodeIncrementalSort.c:1233-1246](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIncrementalSort.c#L1233-L1246)

## Overview
Transfers incremental sort instrumentation statistics from shared memory to private process memory for later analysis and reporting.

## Definition

```c
void
ExecIncrementalSortRetrieveInstrumentation(IncrementalSortState *node)
```
## Detailed Description
This function is responsible for collecting and preserving performance statistics from parallel incremental sort operations. It operates by:

1. Checking if shared instrumentation data exists (shared_info is not NULL)
2. Calculating the total size needed to store all worker statistics
3. Allocating private memory to hold the complete SharedIncrementalSortInfo structure
4. Copying the entire shared memory structure, including statistics from all parallel workers, into the allocated private memory
5. Updating the node's shared_info pointer to reference the private copy

This function is typically called at the end of a parallel incremental sort operation to preserve statistics that would otherwise be lost when the shared memory segment is deallocated. The retrieved statistics can then be used for performance analysis, EXPLAIN output, or other instrumentation purposes.

## Parameters / Member Variables
- `*node`: Pointer to the IncrementalSortState structure from which to retrieve shared instrumentation data
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - memcpy (memory copy operation)
  - offsetof (macro for calculating structure member offsets)
- Data types referenced:
  - [SharedIncrementalSortInfo](../S/SharedIncrementalSortInfo.md)
  - [IncrementalSortInfo](../I/IncrementalSortInfo.md)
- Called from (representative examples):
  - [ExecParallelRetrieveInstrumentation](ExecParallelRetrieveInstrumentation.md) (src/backend/executor/execParallel.c:1068)

## Notes and Other Information
- This function gracefully handles cases where no shared instrumentation exists by returning early if shared_info is NULL
- The size calculation accounts for the variable-length array of per-worker IncrementalSortInfo structures within SharedIncrementalSortInfo
- After this function executes, the statistics are preserved in private memory and will remain accessible even after the parallel query execution context is torn down
- This is part of PostgreSQL's broader instrumentation infrastructure that provides detailed performance metrics for query execution analysis

## Simplified Source

```c
void ExecIncrementalSortRetrieveInstrumentation(IncrementalSortState *node) {
    // Skip if no shared instrumentation data
    if (node->shared_info == NULL)
        return;

    // Calculate total size for SharedIncrementalSortInfo + all worker data
    Size size = offsetof(SharedIncrementalSortInfo, sinfo) +
                node->shared_info->num_workers * sizeof(IncrementalSortInfo);

    // Copy shared memory statistics to private memory for preservation
    SharedIncrementalSortInfo *private_copy = palloc(size);
    memcpy(private_copy, node->shared_info, size);
    node->shared_info = private_copy;
}
```