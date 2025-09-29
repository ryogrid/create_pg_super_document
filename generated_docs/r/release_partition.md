# release_partition

## Location
[src/backend/executor/nodeWindowAgg.c:1335-1384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L1335-L1384)

## Overview
This static function cleans up all partition-local resources including tuplestores, memory contexts, and window function state when finishing processing of a window partition.

## Definition
```c
static void release_partition(WindowAggState *winstate)
```

## Detailed Description
The `release_partition` function performs comprehensive cleanup of resources allocated for processing a window partition. It systematically releases partition-local state for all window functions, resets memory contexts to free allocated memory, and destroys the tuplestore buffer.

The function handles memory management for different types of contexts: the main partition context, aggregate context, and individual per-aggregate contexts when they differ from the main aggregate context. It uses MemoryContextReset rather than retail pfree operations to ensure complete cleanup, particularly for aggregates that may have allocated data without direct pointer tracking.

After cleanup, the function resets the buffer pointer to NULL and marks the partition as not spooled, preparing the WindowAggState for processing the next partition.

## Parameters / Member Variables
- `winstate`: The WindowAggState containing all partition-specific resources to be cleaned up, including function states, memory contexts, and the tuplestore buffer

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [tuplestore_end](../t/tuplestore_end.md)
- Called from (representative examples):
  - [ExecWindowAgg](../E/ExecWindowAgg.md)
  - [ExecEndWindowAgg](../E/ExecEndWindowAgg.md)
  - [ExecReScanWindowAgg](../E/ExecReScanWindowAgg.md)

## Notes and Other Information
- Clears the localmem pointer for all window function objects to invalidate any partition-local state
- Uses MemoryContextReset instead of individual pfree calls to ensure comprehensive memory cleanup, especially for aggregates with indirect allocations
- Handles per-aggregate memory contexts that may differ from the main aggregate context
- Sets buffer to NULL and partition_spooled to false to indicate the partition has been fully released
- This function is essential for preventing memory leaks in queries processing multiple partitions
- Must be called before beginning a new partition to ensure clean state

## Simplified Source

```c
static void release_partition(WindowAggState *winstate) {
    // Clear partition-local state for all window functions
    for (int i = 0; i < winstate->numfuncs; i++) {
        WindowStatePerFunc perfuncstate = &(winstate->perfunc[i]);

        if (perfuncstate->winobj)
            perfuncstate->winobj->localmem = NULL;
    }

    // Reset all partition-local memory contexts
    MemoryContextReset(winstate->partcontext);
    MemoryContextReset(winstate->aggcontext);

    // Reset individual aggregate contexts if they differ from main context
    for (int i = 0; i < winstate->numaggs; i++) {
        if (winstate->peragg[i].aggcontext != winstate->aggcontext)
            MemoryContextReset(winstate->peragg[i].aggcontext);
    }

    // Clean up tuplestore buffer
    if (winstate->buffer)
        tuplestore_end(winstate->buffer);

    // Reset partition state
    winstate->buffer = NULL;
    winstate->partition_spooled = false;
}
```