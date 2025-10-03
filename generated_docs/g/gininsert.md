# gininsert

## Location
[src/backend/access/gin/gininsert.c:483-536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gininsert.c#L483-L536)

## Overview
The gininsert function is the main entry point for inserting a single heap tuple into a GIN index, supporting both normal insertion and fast-update modes.

## Definition

```c
bool
gininsert(Relation index, Datum *values, bool *isnull,
		  ItemPointer ht_ctid, Relation heapRel,
		  IndexUniqueCheck checkUnique,
		  bool indexUnchanged,
		  IndexInfo *indexInfo)
```
## Detailed Description
The gininsert function handles the insertion of a single tuple from the heap relation into the corresponding GIN index. It serves as the main interface for tuple insertion operations and supports two different insertion strategies:

1. **Fast Update Mode**: When enabled, uses ginHeapTupleFastCollect and ginHeapTupleFastInsert to collect entries and batch them for efficient insertion into the pending list
2. **Normal Mode**: Uses ginHeapTupleInsert to directly insert entries into the main index structure

Key operations performed:
- **GinState Management**: Initializes or reuses cached GinState from IndexInfo for efficiency
- **Memory Context Management**: Creates a temporary memory context for insertion operations
- **Multi-Attribute Handling**: Processes all indexed attributes of the tuple
- **Mode Selection**: Automatically chooses between fast-update and normal insertion based on index configuration

## Parameters / Member Variables
- `index`: The GIN index relation into which the tuple will be inserted
- `*values`: Array of Datum values for each indexed attribute of the tuple
- `*isnull`: Array of boolean flags indicating which values are NULL
- `ht_ctid`: ItemPointer (TID) referencing the heap tuple location
- `heapRel`: The heap relation containing the original tuple (may be unused)
- `checkUnique`: Uniqueness check requirement (not relevant for GIN indexes)
- `indexUnchanged`: Whether the indexed values have changed (optimization hint)
- `*indexInfo`: Index metadata structure, also used for caching GinState
## Dependencies
- Functions called/Symbols referenced:
  - [initGinState](../i/initGinState.md)
  - GinGetUseFastUpdate
  - [ginHeapTupleFastCollect](ginHeapTupleFastCollect.md)
  - [ginHeapTupleFastInsert](ginHeapTupleFastInsert.md)
  - [ginHeapTupleInsert](ginHeapTupleInsert.md)
  - AllocSetContextCreate
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [ginhandler](ginhandler.md) (via access method handler)

## Notes and Other Information
- Always returns false since GIN indexes don't support unique constraints
- Caches GinState in IndexInfo->ii_AmCache for performance across multiple calls in the same statement
- Creates and destroys a temporary memory context for each insertion to avoid memory leaks
- Iterates through all indexed attributes (ginstate->origTupdesc->natts) of the tuple
- The choice between fast-update and normal mode is determined by GinGetUseFastUpdate()
- Fast-update mode is more efficient for bulk insertions but may require periodic cleanup
- Uses 1-based attribute numbering when calling helper functions
- Memory context switching ensures proper cleanup even if errors occur during insertion

## Simplified Source

```c
bool
gininsert(Relation index, Datum *values, bool *isnull,
          ItemPointer ht_ctid, Relation heapRel,
          IndexUniqueCheck checkUnique,
          bool indexUnchanged,
          IndexInfo *indexInfo)
{
    GinState *ginstate = (GinState *) indexInfo->ii_AmCache;
    MemoryContext oldCtx, insertCtx;

    // Initialize GinState cache if first call in this statement
    if (ginstate == NULL) {
        oldCtx = MemoryContextSwitchTo(indexInfo->ii_Context);
        ginstate = (GinState *) palloc(sizeof(GinState));
        initGinState(ginstate, index);
        indexInfo->ii_AmCache = (void *) ginstate;
        MemoryContextSwitchTo(oldCtx);
    }

    // Create temporary memory context for insertion
    insertCtx = AllocSetContextCreate(CurrentMemoryContext,
                                      "Gin insert temporary context",
                                      ALLOCSET_DEFAULT_SIZES);
    oldCtx = MemoryContextSwitchTo(insertCtx);

    // Choose insertion method based on fast-update setting
    if (GinGetUseFastUpdate(index)) {
        // Fast-update mode: collect entries and batch insert
        GinTupleCollector collector;
        memset(&collector, 0, sizeof(GinTupleCollector));

        for (int i = 0; i < ginstate->origTupdesc->natts; i++) {
            ginHeapTupleFastCollect(ginstate, &collector,
                                    (OffsetNumber) (i + 1),
                                    values[i], isnull[i], ht_ctid);
        }
        ginHeapTupleFastInsert(ginstate, &collector);
    } else {
        // Normal mode: insert entries directly
        for (int i = 0; i < ginstate->origTupdesc->natts; i++) {
            ginHeapTupleInsert(ginstate, (OffsetNumber) (i + 1),
                               values[i], isnull[i], ht_ctid);
        }
    }

    // Clean up temporary memory context
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insertCtx);

    return false; // GIN indexes don't support unique constraints
}
```