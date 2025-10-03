# gistSortedBuildCallback

## Location
[src/backend/access/gist/gistbuild.c:366-399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuild.c#L366-L399)

## Overview
Callback function used during table scanning for sorted GiST index builds, responsible for processing each heap tuple and adding it to the tuplesort for later bottom-up index construction.

## Definition
```c
static void gistSortedBuildCallback(Relation index, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
```

## Detailed Description
This function serves as the per-tuple callback for table_index_build_scan during sorted GiST index construction. It processes each heap tuple by compressing the index key values using GiST-specific compression functions and then inserting the compressed tuple into the tuplesort state for later sorting and index page construction.

The function operates within the temporary memory context established by the build state to ensure efficient memory management during the scanning process. After processing each tuple, it resets the temporary context to prevent memory accumulation across large numbers of tuples.

The callback increments the build state's tuple counter to track the total number of index entries processed, which is used for final statistics reporting.

## Parameters / Member Variables
- `index`: The GiST index relation being built
- `tid`: ItemPointer (TID) identifying the heap tuple location
- `values`: Array of Datum values for the index key attributes
- `isnull`: Array of null flags corresponding to the values array
- `tupleIsAlive`: Boolean indicating if the tuple is visible (used for HOT updates)
- `state`: Void pointer to GISTBuildState containing build context and tuplesort state

## Dependencies
- Functions called/Symbols referenced:
  - [GISTBuildState](../G/GISTBuildState.md): Build state structure containing context information
  - [gistCompressValues](gistCompressValues.md): Compress index attribute values using GiST compress functions
  - [tuplesort_putindextuplevalues](../t/tuplesort_putindextuplevalues.md): Add compressed tuple values to the tuplesort
  - [MemoryContextReset](../M/MemoryContextReset.md): Reset temporary memory context after processing
- Called from (representative examples):
  - [gistbuild](gistbuild.md): Main GiST build function during sorted build mode

## Notes and Other Information
- This callback is only used when all index attributes have sort support functions available
- The function uses a temporary memory context that is reset after each tuple to prevent memory bloat
- Tuple compression is applied before sorting to ensure the sorted order matches the final index structure
- The tupleIsAlive parameter is handled by the tuplesort infrastructure for MVCC compliance
- Statistics tracking via indtuples counter provides feedback for query planner cost estimation

## Simplified Source

```c
static void
gistSortedBuildCallback(Relation index, ItemPointer tid, Datum *values,
                       bool *isnull, bool tupleIsAlive, void *state)
{
    GISTBuildState *buildstate = (GISTBuildState *) state;
    MemoryContext oldCtx;
    Datum compressed_values[INDEX_MAX_KEYS];

    // Switch to temporary context for memory management
    oldCtx = MemoryContextSwitchTo(buildstate->giststate->tempCxt);

    // Compress index key values using GiST compress functions
    gistCompressValues(buildstate->giststate, index, values, isnull,
                      true, compressed_values);

    // Add compressed tuple to tuplesort for later sorting
    tuplesort_putindextuplevalues(buildstate->sortstate, buildstate->indexrel,
                                 tid, compressed_values, isnull);

    // Clean up temporary memory and update statistics
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->giststate->tempCxt);
    buildstate->indtuples += 1;
}
```