# ginBuildCallback

## Location
[src/backend/access/gin/gininsert.c:277-316](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gininsert.c#L277-L316)

## Overview
Callback function used during GIN index creation that processes each heap tuple and manages memory usage during bulk insertion operations.

## Definition

```c
static void
ginBuildCallback(Relation index, ItemPointer tid, Datum *values,
				 bool *isnull, bool tupleIsAlive, void *state)
```
## Detailed Description
This function serves as the callback mechanism for PostgreSQL's index build infrastructure during GIN index creation. It is called once for each heap tuple being indexed and performs the following operations:

1. **Multi-attribute processing**: Iterates through all indexed attributes of the tuple, calling ginHeapTupleBulkInsert for each attribute to extract and accumulate index entries.

2. **Memory management**: Monitors the BuildAccumulator's memory usage and triggers a flush when it reaches the maintenance_work_mem threshold to prevent excessive memory consumption.

3. **Batch processing**: When memory limits are reached, it processes all accumulated entries by scanning the BuildAccumulator and calling ginEntryInsert for each unique key, then resets the accumulator for the next batch.

4. **Interruption handling**: Includes CHECK_FOR_INTERRUPTS() to allow cancellation during long-running index builds.

The function implements PostgreSQL's standard pattern for bulk index creation, balancing memory usage with insertion efficiency through batched operations.

## Parameters / Member Variables
- `index`: The GIN index relation being built
- `tid`: ItemPointer (TID) of the current heap tuple being processed
- `*values`: Array of attribute values from the heap tuple
- `*isnull`: Array of null flags corresponding to the values
- `tupleIsAlive`: Flag indicating if the tuple is visible (used for partial index builds)
- `*state`: Opaque pointer to GinBuildState structure containing build context
## Dependencies
- Functions called/Symbols referenced:
  - [ginHeapTupleBulkInsert](ginHeapTupleBulkInsert.md): Process each attribute value for bulk insertion
  - [ginBeginBAScan](ginBeginBAScan.md): Initialize scan of BuildAccumulator contents
  - [ginGetBAEntry](ginGetBAEntry.md): Retrieve next accumulated entry for processing
  - [ginEntryInsert](ginEntryInsert.md): Insert accumulated entries into the actual index
  - [ginInitBA](ginInitBA.md): Reset BuildAccumulator after flush
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md): Manage memory contexts for clean operation
  - [MemoryContextReset](../M/MemoryContextReset.md): Reset temporary memory context
  - CHECK_FOR_INTERRUPTS: Allow cancellation of long operations

- Called from (representative examples):
  - [ginbuild](ginbuild.md): Main index build function via IndexBuildHeapScan

## Notes and Other Information
- Follows PostgreSQL's standard callback pattern for index building (IndexBuildCallback signature)
- Uses maintenance_work_mem setting to control memory usage during index builds
- The tupleIsAlive parameter allows for building indexes on live tables with concurrent activity
- Implements efficient bulk loading strategy by batching insertions
- Memory context switching ensures clean memory management during bulk operations
- Critical for GIN index build performance, especially on large tables
- The static keyword indicates this is internal to the GIN access method implementation
- Part of PostgreSQL's pluggable index access method architecture

## Simplified Source
```c
static void ginBuildCallback(Relation index, ItemPointer tid, Datum *values,
                            bool *isnull, bool tupleIsAlive, void *state) {
    GinBuildState *buildstate = (GinBuildState *) state;
    MemoryContext oldCtx;
    int i;

    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    // Process each indexed attribute of the tuple
    for (i = 0; i < buildstate->ginstate.origTupdesc->natts; i++) {
        ginHeapTupleBulkInsert(buildstate, (OffsetNumber) (i + 1),
                              values[i], isnull[i], tid);
    }

    // Check if we've exceeded memory limit - flush if needed
    if (buildstate->accum.allocatedMemory >= (Size) maintenance_work_mem * 1024L) {
        ItemPointerData *list;
        Datum key;
        GinNullCategory category;
        uint32 nlist;
        OffsetNumber attnum;

        // Scan all accumulated entries and insert them into index
        ginBeginBAScan(&buildstate->accum);
        while ((list = ginGetBAEntry(&buildstate->accum,
                                    &attnum, &key, &category, &nlist)) != NULL) {
            // Allow interruption during long operations
            CHECK_FOR_INTERRUPTS();

            ginEntryInsert(&buildstate->ginstate, attnum, key, category,
                          list, nlist, &buildstate->buildStats);
        }

        // Reset accumulator for next batch
        MemoryContextReset(buildstate->tmpCtx);
        ginInitBA(&buildstate->accum);
    }

    MemoryContextSwitchTo(oldCtx);
}
```