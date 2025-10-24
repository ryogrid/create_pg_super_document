# brininsertcleanup

## Location
[src/backend/access/brin/brin.c:503-529](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L503-L529)

## Overview
The `brininsertcleanup` function serves as a callback to clean up the `BrinInsertState` structure and release associated resources once all tuple insertions for a command are completed.

## Definition
```c
void brininsertcleanup(Relation index, IndexInfo *indexInfo)
```

## Detailed Description
This function is called by the PostgreSQL index access method infrastructure to perform cleanup after a series of tuple insertions has completed. It is responsible for properly deallocating the `BrinInsertState` structure that was cached in the `IndexInfo` to optimize performance across multiple insertions.

The function follows a careful cleanup sequence:
1. First, it clears the cached pointer in `IndexInfo->ii_AmCache` to avoid dangling pointer issues if cleanup fails partway through
2. Terminates the revmap access structure using `brinRevmapTerminate()`
3. Frees the `BrinInsertState` structure itself

The BRIN descriptor (`bis_desc`) does not need explicit cleanup as it was allocated in its own memory context and will be automatically cleaned up when that context is destroyed.

## Parameters / Member Variables
- `index`: The BRIN index relation that was being inserted into
- `indexInfo`: Index information structure containing the cached insertion state in `ii_AmCache`

The `BrinInsertState` structure being cleaned up contains:
- `bis_rmAccess`: Revmap access structure that needs explicit termination
- `bis_desc`: BRIN descriptor (cleaned up automatically via memory context)
- `bis_pages_per_range`: Pages per range value (no cleanup needed)

## Dependencies
- Functions called/Symbols referenced:
  - [brinRevmapTerminate](brinRevmapTerminate.md)() (terminates revmap access structure)
  - [pfree](../p/pfree.md)() (frees the insertion state structure)
  - [BrinInsertState](../B/BrinInsertState.md) (structure type)
  - `[IndexInfo](../I/IndexInfo.md)` (structure type)

- Called from (representative examples):
  - PostgreSQL index access method infrastructure (via `brinhandler()`)
  - End of batch insertion operations

## Notes and Other Information
- This is a cleanup callback function registered in the `IndexAmRoutine` structure
- The function is defensive - it checks if the cache is already NULL before proceeding
- The cleanup order is important: clearing the cache pointer first prevents dangling pointer issues
- The BRIN descriptor cleanup is handled automatically via PostgreSQL memory context management
- This function ensures that resources allocated during `initialize_brin_insertstate()` are properly released
- Called once per command, not once per tuple insertion, making it efficient for bulk operations

## Simplified Source

```c
void
brininsertcleanup(Relation index, IndexInfo *indexInfo)
{
    BrinInsertState *bistate = (BrinInsertState *) indexInfo->ii_AmCache;

    // Exit early if no cached state
    if (bistate == NULL)
        return;

    // Clear cache pointer first to avoid dangling references
    indexInfo->ii_AmCache = NULL;

    // Clean up revmap access structure
    brinRevmapTerminate(bistate->bis_rmAccess);

    // Free the insertion state structure
    pfree(bistate);
}
```