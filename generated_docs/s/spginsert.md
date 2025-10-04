# spginsert

## Location
[src/backend/access/spgist/spginsert.c:183-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spginsert.c#L183-L219)

## Overview
Inserts a single new tuple into an existing SP-GiST index, handling potential conflicts and memory management.

## Definition

```c
bool
spginsert(Relation index, Datum *values, bool *isnull,
		  ItemPointer ht_ctid, Relation heapRel,
		  IndexUniqueCheck checkUnique,
		  bool indexUnchanged,
		  IndexInfo *indexInfo)
```
## Detailed Description
This function handles the insertion of individual tuples into an SP-GiST index during normal database operations (as opposed to bulk building). It creates a temporary memory context for the insertion process and implements retry logic to handle concurrent insertion conflicts. The function repeatedly calls spgdoinsert() until the insertion succeeds, resetting the memory context and reinitializing the SP-GiST state on each retry to handle conflicts with concurrent operations. After successful insertion, it updates the index metapage and cleans up the temporary context.

## Parameters / Member Variables
- `index`: The SP-GiST index relation to insert into
- `*values`: Array of column values for the new tuple
- `*isnull`: Array of boolean flags indicating NULL values
- `ht_ctid`: Heap tuple ID (item pointer) of the new tuple
- `heapRel`: The heap relation containing the tuple
- `checkUnique`: Unique constraint checking mode (unused in SP-GiST)
- `indexUnchanged`: Whether the indexed values are unchanged (for HOT updates)
- `*indexInfo`: Index metadata and configuration
## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [initSpGistState](../i/initSpGistState.md)
  - [spgdoinsert](spgdoinsert.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [SpGistUpdateMetaPage](../S/SpGistUpdateMetaPage.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [SpGistState](../S/SpGistState.md)
- Called from (representative examples):
  - [spghandler](spghandler.md)

## Notes and Other Information
Always returns false since SP-GiST does not support unique constraints. The retry mechanism ensures eventual success even under high concurrency. Memory context management prevents memory leaks during repeated retry attempts.

## Simplified Source

```c
bool spginsert(Relation index, Datum *values, bool *isnull,
               ItemPointer ht_ctid, Relation heapRel,
               IndexUniqueCheck checkUnique,
               bool indexUnchanged,
               IndexInfo *indexInfo) {
    SpGistState spgstate;
    MemoryContext oldCtx;
    MemoryContext insertCtx;

    // Create temporary memory context for insertion
    insertCtx = AllocSetContextCreate(CurrentMemoryContext,
                                     "SP-GiST insert temporary context",
                                     ALLOCSET_DEFAULT_SIZES);
    oldCtx = MemoryContextSwitchTo(insertCtx);

    // Initialize SP-GiST state
    initSpGistState(&spgstate, index);

    // Retry insertion until successful (handling concurrent conflicts)
    while (!spgdoinsert(index, &spgstate, ht_ctid, values, isnull)) {
        MemoryContextReset(insertCtx);      // Reset context on retry
        initSpGistState(&spgstate, index);  // Reinitialize state
    }

    // Update metapage and cleanup
    SpGistUpdateMetaPage(index);
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insertCtx);

    // SP-GiST doesn't support unique constraints
    return false;
}
```