# brinbuild

## Location
[src/backend/access/brin/brin.c:1095-1263](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L1095-L1263)

## Overview
The main function responsible for building a new BRIN (Block Range Index) from scratch, including metadata initialization, tuple scanning, and optional parallel processing.

## Definition

```c
IndexBuildResult *
brinbuild(Relation heap, Relation index, IndexInfo *indexInfo)
```
## Detailed Description
 is the primary index construction function for BRIN indexes. It performs a complete build of a new BRIN index by:

1. **Initialization Phase**: Creates and initializes the metadata page with version and pages-per-range information
2. **WAL Logging**: Records the index creation in write-ahead log if needed for crash recovery
3. **State Setup**: Initializes build state including revmap (reverse mapping) and tuple processing structures
4. **Parallel Processing**: Optionally launches parallel workers to scan different portions of the heap table
5. **Data Processing**: Either merges results from parallel workers or performs serial table scan using 
6. **Range Completion**: Fills empty ranges and finalizes index construction
7. **Cleanup**: Releases resources and returns build statistics

The function supports both serial and parallel index building modes, with parallel mode being chosen based on the  setting in .

## Parameters / Member Variables
- `heap`: The heap relation (table) being indexed
- `index`: The BRIN index relation being built
- `*indexInfo`: Contains index metadata including parallel worker configuration and concurrent build settings
## Dependencies
- Functions called/Symbols referenced:
  - : Get table size in blocks
  - : Extend relation with new block
  - : Initialize BRIN metadata page
  - : Get pages per range setting
  - : Initialize reverse mapping structure
  - : Set up build state
  - : Start parallel index build
  - : Merge parallel worker results
  - : Perform table scan for index building
  - : Process each tuple during scan
  - : Create and insert index tuples
  - : Fill ranges with no data
  - : Clean up reverse mapping
  - : Clean up build state
- Called from (representative examples):
  - : BRIN access method handler function

## Notes and Other Information
- Expects to be called on an empty index relation (throws error if blocks exist)
- Critical sections not required as relation creation rollback handles errors
- Parallel building requires sufficient  (32MB per worker by default)
- Uses physical order scanning (no syncscan) to ensure proper range generation from block 0
- Supports concurrent index builds when  is set
- WAL logging ensures crash recovery consistency for permanent indexes
- Returns  with heap tuple count and index tuple count statistics

## Simplified Source

```c
IndexBuildResult *brinbuild(Relation heap, Relation index, IndexInfo *indexInfo) {
    // Verify we're building on an empty index
    if (RelationGetNumberOfBlocks(index) != 0)
        elog(ERROR, "index \"%s\" already contains data", RelationGetRelationName(index));

    // Create and initialize metadata page
    Buffer meta = ExtendBufferedRel(BMR_REL(index), MAIN_FORKNUM, NULL,
                                   EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK);
    brin_metapage_init(BufferGetPage(meta), BrinGetPagesPerRange(index), BRIN_CURRENT_VERSION);
    MarkBufferDirty(meta);

    // WAL logging if needed
    if (RelationNeedsWAL(index)) {
        xl_brin_createidx xlrec;
        xlrec.version = BRIN_CURRENT_VERSION;
        xlrec.pagesPerRange = BrinGetPagesPerRange(index);

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfBrinCreateIdx);
        XLogRegisterBuffer(0, meta, REGBUF_WILL_INIT | REGBUF_STANDARD);
        XLogRecPtr recptr = XLogInsert(RM_BRIN_ID, XLOG_BRIN_CREATE_INDEX);
        PageSetLSN(BufferGetPage(meta), recptr);
    }
    UnlockReleaseBuffer(meta);

    // Initialize build state
    BlockNumber pagesPerRange;
    BrinRevmap *revmap = brinRevmapInitialize(index, &pagesPerRange);
    BrinBuildState *state = initialize_brin_buildstate(index, revmap, pagesPerRange,
                                                      RelationGetNumberOfBlocks(heap));

    double reltuples;

    // Choose parallel or serial build
    if (indexInfo->ii_ParallelWorkers > 0) {
        _brin_begin_parallel(state, heap, index, indexInfo->ii_Concurrent,
                            indexInfo->ii_ParallelWorkers);
    }

    if (state->bs_leader) {
        // Parallel build: coordinate workers and merge results
        SortCoordinate coordinate = palloc0(sizeof(SortCoordinateData));
        coordinate->isWorker = false;
        coordinate->nParticipants = state->bs_leader->nparticipanttuplesorts;
        coordinate->sharedsort = state->bs_leader->sharedsort;

        state->bs_sortstate = tuplesort_begin_index_brin(maintenance_work_mem,
                                                        coordinate, TUPLESORT_NONE);
        reltuples = _brin_parallel_merge(state);
        _brin_end_parallel(state->bs_leader, state);
    } else {
        // Serial build: scan table directly
        reltuples = table_index_build_scan(heap, index, indexInfo, false, true,
                                          brinbuildCallback, (void *) state, NULL);

        // Process final batch and fill empty ranges
        form_and_insert_tuple(state);
        brin_fill_empty_ranges(state, state->bs_currRangeStart, state->bs_maxRangeStart);
    }

    // Cleanup and return statistics
    double idxtuples = state->bs_numtuples;
    brinRevmapTerminate(state->bs_rmAccess);
    terminate_brin_buildstate(state);

    IndexBuildResult *result = palloc_object(IndexBuildResult);
    result->heap_tuples = reltuples;
    result->index_tuples = idxtuples;
    return result;
}
```