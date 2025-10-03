# gistbuild

## Location
[src/backend/access/gist/gistbuild.c:179-365](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuild.c#L179-L365)

## Overview
Main entry point function for building GiST (Generalized Search Tree) indexes, responsible for orchestrating the entire index construction process with support for multiple build strategies including sorted builds and buffered builds.

## Definition

```c
IndexBuildResult *
gistbuild(Relation heap, Relation index, IndexInfo *indexInfo)
```
## Detailed Description
The gistbuild function is the primary interface for GiST index construction, implementing a sophisticated strategy selection mechanism to optimize build performance based on available operators and user configuration. The function supports three main build modes:

1. **GIST_SORTED_BUILD**: Uses tuplesort when all key attributes have sort support functions, building the index bottom-up from pre-sorted data
2. **GIST_BUFFERING_STATS/GIST_BUFFERING_ACTIVE**: Uses buffering to optimize intermediate-level insertions during top-down construction  
3. **GIST_BUFFERING_DISABLED**: Traditional tuple-by-tuple insertion without buffering

The function automatically detects the optimal build strategy by checking for sort support functions, unless explicitly overridden by user options. For sorted builds, it leverages the tuplesort infrastructure to pre-sort all tuples before building pages bottom-up, which can be significantly more efficient for large datasets.

## Parameters / Member Variables
- `heap`: Source relation containing the data to be indexed
- `index`: Target index relation being built
- `*indexInfo`: Metadata about the index including key attributes and predicates
## Dependencies
- Functions called/Symbols referenced:
  - [initGISTstate](../i/initGISTstate.md): Initialize GiST operational state
  - [createTempGistContext](../c/createTempGistContext.md): Create temporary memory context for tuple processing
  - [index_getprocid](../i/index_getprocid.md): Retrieve sort support function OIDs
  - [tuplesort_begin_index_gist](../t/tuplesort_begin_index_gist.md): Initialize tuplesort for GiST index building
  - [table_index_build_scan](../t/table_index_build_scan.md): Scan heap relation and process tuples
  - [gistSortedBuildCallback](gistSortedBuildCallback.md): Callback for sorted build tuple processing
  - [gist_indexsortbuild](gist_indexsortbuild.md): Build index pages from sorted tuples
  - [gistBuildCallback](gistBuildCallback.md): Callback for traditional insertion build
  - [gistNewBuffer](gistNewBuffer.md): Allocate new index page buffer
  - [GISTInitBuffer](../G/GISTInitBuffer.md): Initialize GiST page structure
- Called from (representative examples):
  - [gisthandler](gisthandler.md): GiST access method handler dispatch function

## Notes and Other Information
- The function expects to be called exactly once per index relation and will error if the index already contains data
- Build mode selection prioritizes sorted builds when all attributes have sort support, falling back to buffering strategies otherwise
- Memory management uses a temporary context that is reset after each tuple to prevent memory bloat during large index builds
- WAL logging is deferred until the end of construction for non-sorted builds to improve performance
- The function returns statistics about heap tuples processed and index tuples created for use by the query planner

## Simplified Source

```c
IndexBuildResult *
gistbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result;
    double reltuples;
    GISTBuildState buildstate;
    int fillfactor;
    GiSTOptions *options = (GiSTOptions *) index->rd_options;

    // Ensure index is empty before building
    if (RelationGetNumberOfBlocks(index) != 0)
        elog(ERROR, "index \"%s\" already contains data",
             RelationGetRelationName(index));

    // Initialize build state
    buildstate.indexrel = index;
    buildstate.heaprel = heap;
    buildstate.giststate = initGISTstate(index);
    buildstate.giststate->tempCxt = createTempGistContext();

    // Determine build strategy based on options and capabilities
    if (options && options->buffering_mode == GIST_OPTION_BUFFERING_ON)
        buildstate.buildMode = GIST_BUFFERING_STATS;
    else if (options && options->buffering_mode == GIST_OPTION_BUFFERING_OFF)
        buildstate.buildMode = GIST_BUFFERING_DISABLED;
    else
        buildstate.buildMode = GIST_BUFFERING_AUTO;

    // Check if sorted build is possible (all attributes have sort support)
    if (buildstate.buildMode != GIST_BUFFERING_STATS)
    {
        bool hasallsortsupports = true;
        int keyscount = IndexRelationGetNumberOfKeyAttributes(index);

        for (int i = 0; i < keyscount; i++)
        {
            if (!OidIsValid(index_getprocid(index, i + 1, GIST_SORTSUPPORT_PROC)))
            {
                hasallsortsupports = false;
                break;
            }
        }
        if (hasallsortsupports)
            buildstate.buildMode = GIST_SORTED_BUILD;
    }

    // Calculate free space based on fill factor
    fillfactor = options ? options->fillfactor : GIST_DEFAULT_FILLFACTOR;
    buildstate.freespace = BLCKSZ * (100 - fillfactor) / 100;
    buildstate.indtuples = 0;

    if (buildstate.buildMode == GIST_SORTED_BUILD)
    {
        // Sorted build: sort all data then build bottom-up
        buildstate.sortstate = tuplesort_begin_index_gist(heap, index,
                                                         maintenance_work_mem, NULL,
                                                         TUPLESORT_NONE);

        // Scan heap and add tuples to sort
        reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
                                          gistSortedBuildCallback, &buildstate, NULL);

        // Sort and build index pages
        tuplesort_performsort(buildstate.sortstate);
        gist_indexsortbuild(&buildstate);
        tuplesort_end(buildstate.sortstate);
    }
    else
    {
        // Traditional build: initialize empty index and insert tuples
        Buffer buffer = gistNewBuffer(index, heap);
        Page page = BufferGetPage(buffer);

        START_CRIT_SECTION();
        GISTInitBuffer(buffer, F_LEAF);
        MarkBufferDirty(buffer);
        PageSetLSN(page, GistBuildLSN);
        UnlockReleaseBuffer(buffer);
        END_CRIT_SECTION();

        // Scan and insert tuples
        reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
                                          gistBuildCallback, &buildstate, NULL);

        // Flush buffered tuples if buffering was used
        if (buildstate.buildMode == GIST_BUFFERING_ACTIVE)
        {
            gistEmptyAllBuffers(&buildstate);
            gistFreeBuildBuffers(buildstate.gfbb);
        }

        // Write WAL records for entire index if needed
        if (RelationNeedsWAL(index))
            log_newpage_range(index, MAIN_FORKNUM, 0,
                             RelationGetNumberOfBlocks(index), true);
    }

    // Cleanup and return statistics
    MemoryContextDelete(buildstate.giststate->tempCxt);
    freeGISTstate(buildstate.giststate);

    result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
    result->heap_tuples = reltuples;
    result->index_tuples = (double) buildstate.indtuples;

    return result;
}
```