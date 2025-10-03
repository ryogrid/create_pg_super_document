# ginHeapTupleBulkInsert

## Location
[src/backend/access/gin/gininsert.c:253-276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gininsert.c#L253-L276)

## Overview
Extracts index entries from a single indexable item and adds them to the BuildAccumulator during initial GIN index creation.

## Definition

```c
static void
ginHeapTupleBulkInsert(GinBuildState *buildstate, OffsetNumber attnum,
					   Datum value, bool isNull,
					   ItemPointer heapptr)
```
## Detailed Description
This function is specifically designed for use during initial index creation (bulk operations). It processes a single heap tuple attribute by:

1. **Entry extraction**: Uses the appropriate extraction function to convert the input value into a set of indexable entries (keys). This respects the data type's specific extraction logic.

2. **Memory management**: Switches to a temporary memory context during entry extraction to ensure clean memory handling, then resets this context after processing to prevent memory accumulation.

3. **Bulk accumulation**: Adds the extracted entries to the BuildAccumulator structure, which batches insertions for efficient bulk loading.

4. **Statistics tracking**: Increments the total count of index tuples being created.

The function is optimized for bulk operations during index creation, using the BuildAccumulator pattern to defer actual index insertions until efficient batch sizes are reached.

## Parameters / Member Variables
- : State structure for index build operations containing accumulator and context
- : Attribute number being indexed (for multi-column indexes)
- : The actual data value to be indexed
- : Flag indicating whether the value is NULL
- : Pointer to the heap tuple (TID) that contains this value

## Dependencies
- Functions called/Symbols referenced:
  - [ginExtractEntries](ginExtractEntries.md): Extract indexable keys from the input value using type-specific logic
  - [ginInsertBAEntries](ginInsertBAEntries.md): Add extracted entries to BuildAccumulator for batched insertion
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md): Switch memory contexts for clean memory management
  - [MemoryContextReset](../M/MemoryContextReset.md): Reset temporary memory context to prevent accumulation

- Called from (representative examples):
  - [ginBuildCallback](ginBuildCallback.md): Main callback function during index build operations

## Notes and Other Information
- Used exclusively during initial index creation, not for regular insertions
- The static keyword indicates this is an internal implementation detail
- Memory context switching ensures that extraction operations don't leak memory during bulk operations
- [BuildAccumulator](../B/BuildAccumulator.md) pattern allows for efficient batching of insertions during index build
- Updates indtuples counter to track total number of index entries being created
- Part of PostgreSQL's strategy for efficient bulk index creation
- The funcCtx memory context is reset after each tuple to prevent memory bloat during large index builds

## Simplified Source
```c
static void ginHeapTupleBulkInsert(GinBuildState *buildstate, OffsetNumber attnum,
                                  Datum value, bool isNull,
                                  ItemPointer heapptr) {
    Datum *entries;
    GinNullCategory *categories;
    int32 nentries;
    MemoryContext oldCtx;

    // Switch to temporary context for entry extraction
    oldCtx = MemoryContextSwitchTo(buildstate->funcCtx);

    // Extract indexable entries from the input value
    entries = ginExtractEntries(buildstate->accum.ginstate, attnum,
                               value, isNull,
                               &nentries, &categories);

    MemoryContextSwitchTo(oldCtx);

    // Add entries to build accumulator for batched insertion
    ginInsertBAEntries(&buildstate->accum, heapptr, attnum,
                      entries, categories, nentries);

    // Track total number of index tuples being created
    buildstate->indtuples += nentries;

    // Reset temporary context to prevent memory accumulation
    MemoryContextReset(buildstate->funcCtx);
}
```