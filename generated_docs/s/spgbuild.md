# spgbuild

## Location
[src/backend/access/spgist/spginsert.c:73-153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spginsert.c#L73-L153)

## Overview
The main function responsible for building a complete SP-GiST index from scratch by initializing the index structure and inserting all heap tuples.

## Definition

```c
IndexBuildResult *
spgbuild(Relation heap, Relation index, IndexInfo *indexInfo)
```

## Detailed Description
This function implements the complete SP-GiST index building process. It first validates that the index is empty, then initializes the fundamental index pages (metapage, root page, and null-tuples page). After setting up the basic structure, it scans all heap tuples using table_index_build_scan() with spgistBuildCallback() to insert each tuple into the index. The function handles WAL logging requirements and returns statistics about the build process. It uses a temporary memory context during the build to manage memory efficiently.

## Parameters / Member Variables
- `heap`: The heap relation being indexed
- `index`: The SP-GiST index relation being built
- `indexInfo`: Index metadata and configuration information

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfBlocks
  - [SpGistNewBuffer](../S/SpGistNewBuffer.md)
  - [SpGistInitMetapage](../S/SpGistInitMetapage.md)
  - [SpGistInitBuffer](../S/SpGistInitBuffer.md)
  - [initSpGistState](../i/initSpGistState.md)
  - [table_index_build_scan](../t/table_index_build_scan.md)
  - [spgistBuildCallback](spgistBuildCallback.md)
  - [SpGistUpdateMetaPage](../S/SpGistUpdateMetaPage.md)
  - RelationNeedsWAL
  - [log_newpage_range](../l/log_newpage_range.md)
- Called from (representative examples):
  - [spghandler](spghandler.md)

## Notes and Other Information
The function ensures index pages are created in the correct order (meta, root, null) and validates their block numbers. It includes comprehensive WAL logging for durability when required. The build process is atomic and creates a fully functional SP-GiST index ready for queries.

## Simplified Source

```c
IndexBuildResult *spgbuild(Relation heap, Relation index, IndexInfo *indexInfo) {
    IndexBuildResult *result;
    double reltuples;
    SpGistBuildState buildstate;
    Buffer metabuffer, rootbuffer, nullbuffer;

    // Ensure index is empty
    if (RelationGetNumberOfBlocks(index) != 0)
        elog(ERROR, "index \"%s\" already contains data", RelationGetRelationName(index));

    // Initialize fundamental index pages
    metabuffer = SpGistNewBuffer(index);   // Block 0: metapage
    rootbuffer = SpGistNewBuffer(index);   // Block 1: root page
    nullbuffer = SpGistNewBuffer(index);   // Block 2: null page

    // Verify correct block numbers
    Assert(BufferGetBlockNumber(metabuffer) == SPGIST_METAPAGE_BLKNO);
    Assert(BufferGetBlockNumber(rootbuffer) == SPGIST_ROOT_BLKNO);
    Assert(BufferGetBlockNumber(nullbuffer) == SPGIST_NULL_BLKNO);

    // Initialize page contents in critical section
    START_CRIT_SECTION();
    SpGistInitMetapage(BufferGetPage(metabuffer));
    MarkBufferDirty(metabuffer);
    SpGistInitBuffer(rootbuffer, SPGIST_LEAF);
    MarkBufferDirty(rootbuffer);
    SpGistInitBuffer(nullbuffer, SPGIST_LEAF | SPGIST_NULLS);
    MarkBufferDirty(nullbuffer);
    END_CRIT_SECTION();

    // Release initial buffers
    UnlockReleaseBuffer(metabuffer);
    UnlockReleaseBuffer(rootbuffer);
    UnlockReleaseBuffer(nullbuffer);

    // Setup build state
    initSpGistState(&buildstate.spgstate, index);
    buildstate.spgstate.isBuild = true;
    buildstate.indtuples = 0;
    buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
                                             "SP-GiST build temporary context",
                                             ALLOCSET_DEFAULT_SIZES);

    // Scan heap and insert all tuples using callback
    reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
                                      spgistBuildCallback, (void *) &buildstate,
                                      NULL);

    // Cleanup and finalize
    MemoryContextDelete(buildstate.tmpCtx);
    SpGistUpdateMetaPage(index);

    // Write WAL records if needed
    if (RelationNeedsWAL(index)) {
        log_newpage_range(index, MAIN_FORKNUM, 0, RelationGetNumberOfBlocks(index), true);
    }

    // Return build statistics
    result = (IndexBuildResult *) palloc0(sizeof(IndexBuildResult));
    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;

    return result;
}
```