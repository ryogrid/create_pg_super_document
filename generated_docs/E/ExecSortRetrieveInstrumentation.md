# ExecSortRetrieveInstrumentation

## Location
[src/backend/executor/nodeSort.c:476-489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSort.c#L476-L489)

## Overview
Transfers tuplesort instrumentation statistics from Dynamic Shared Memory (DSM) to private memory for post-execution analysis and reporting.

## Definition
```c
void ExecSortRetrieveInstrumentation(SortState *node)
```

## Detailed Description
ExecSortRetrieveInstrumentation is responsible for collecting and preserving tuplesort performance statistics that were gathered from parallel worker processes during query execution. This function is typically called after parallel execution completes, when the leader process needs to consolidate statistics from all workers.

The function performs the following operations:
- Checks if shared instrumentation data exists (returns early if not)
- Calculates the total size needed to store the SharedSortInfo header and all worker instrumentation data
- Allocates private memory to hold a complete copy of the shared instrumentation data
- Copies the entire shared memory contents to the newly allocated private memory
- Updates the node's shared_info pointer to reference the private copy

This ensures that instrumentation data remains available for analysis and reporting even after the shared memory segments are destroyed when parallel execution ends.

## Parameters / Member Variables
- `node`: Pointer to the SortState containing the reference to shared instrumentation data that will be copied to private memory

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (allocates private memory)
  - memcpy (copies memory contents)
  - [SharedSortInfo](../S/SharedSortInfo.md) (shared sort information structure)
  - [TuplesortInstrumentation](../T/TuplesortInstrumentation.md) (instrumentation data structure)
- Called from (representative examples):
  - [ExecParallelRetrieveInstrumentation](ExecParallelRetrieveInstrumentation.md) (parallel execution instrumentation collector)

## Notes and Other Information
- The function safely handles cases where no shared instrumentation exists by returning early
- Memory allocation size includes both the SharedSortInfo header and the array of worker instrumentation structures
- After this function completes, the instrumentation data persists in private memory and can be accessed for reporting
- The private memory copy preserves all statistics from all workers, enabling comprehensive performance analysis
- This function is the final step in the parallel sort instrumentation lifecycle, moving from shared to private storage

## Simplified Source

```c
void ExecSortRetrieveInstrumentation(SortState *node) {
    // Skip if no shared instrumentation data
    if (node->shared_info == NULL)
        return;

    // Calculate total size for SharedSortInfo + all worker instrumentation data
    Size size = offsetof(SharedSortInfo, sinstrument) +
                node->shared_info->num_workers * sizeof(TuplesortInstrumentation);

    // Copy shared memory statistics to private memory for persistence
    SharedSortInfo *private_copy = palloc(size);
    memcpy(private_copy, node->shared_info, size);
    node->shared_info = private_copy;
}
```