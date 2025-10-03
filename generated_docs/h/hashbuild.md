# hashbuild

## Location
[src/backend/access/hash/hash.c:115-200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash.c#L115-L200)

## Overview
Builds a new hash index by scanning the heap relation and inserting all tuples into the newly created hash index structure.

## Definition
```c
IndexBuildResult *hashbuild(Relation heap, Relation index, IndexInfo *indexInfo)
```

## Detailed Description
The hashbuild function is responsible for creating a complete hash index from scratch. It performs several key operations: estimates the number of tuples in the heap relation, initializes the hash index metadata and initial buckets, and then scans the entire heap to insert all valid tuples into the index.

The function includes an optimization for large indexes that might not fit in memory. If the estimated index size exceeds maintenance_work_mem or the number of available buffers, it uses a spooling mechanism to sort tuples by their expected bucket number before insertion. This prevents thrashing when the index is larger than available RAM by improving locality of access.

The build process uses the table_index_build_scan function with hashbuildCallback to process each tuple during the heap scan. Progress is tracked and reported through the PostgreSQL progress reporting system.

## Parameters / Member Variables
- `heap`: The heap relation being indexed
- `index`: The hash index relation being built
- `indexInfo`: Index configuration and metadata information

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfBlocks
  - [estimate_rel_size](../e/estimate_rel_size.md)
  - [_hash_init](_hash_init.md)
  - [_h_spoolinit](_h_spoolinit.md)
  - [table_index_build_scan](../t/table_index_build_scan.md)
  - [hashbuildCallback](hashbuildCallback.md)
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - [_h_indexbuild](_h_indexbuild.md)
  - [_h_spooldestroy](_h_spooldestroy.md)
- Called from:
  - [hashhandler](hashhandler.md) (as amroutine->ambuild callback)
  - Index creation system

## Notes and Other Information
- Ensures the index relation is empty before building (throws ERROR if not)
- Uses a sophisticated sorting strategy for large indexes to prevent memory thrashing
- The sort threshold is determined by maintenance_work_mem and buffer pool size
- Temporary relations use NLocBuffer instead of NBuffers for the threshold calculation
- Returns statistics including the number of heap tuples scanned and index tuples created
- The function handles both sorted and unsorted insertion paths depending on index size

## Simplified Source

```c
IndexBuildResult *hashbuild(Relation heap, Relation index, IndexInfo *indexInfo) {
    IndexBuildResult *result;
    BlockNumber relpages;
    double reltuples, allvisfrac;
    uint32 num_buckets;
    long sort_threshold;
    HashBuildState buildstate;

    // Verify index is empty
    if (RelationGetNumberOfBlocks(index) != 0)
        elog(ERROR, "index \"%s\" already contains data", RelationGetRelationName(index));

    // Estimate heap size and initialize hash index
    estimate_rel_size(heap, NULL, &relpages, &reltuples, &allvisfrac);
    num_buckets = _hash_init(index, reltuples, MAIN_FORKNUM);

    // Determine if we should sort tuples to prevent thrashing
    sort_threshold = (maintenance_work_mem * 1024L) / BLCKSZ;
    if (index->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
        sort_threshold = Min(sort_threshold, NBuffers);
    else
        sort_threshold = Min(sort_threshold, NLocBuffer);

    // Setup spooling if index is large
    if (num_buckets >= (uint32) sort_threshold)
        buildstate.spool = _h_spoolinit(heap, index, num_buckets);
    else
        buildstate.spool = NULL;

    // Initialize build state and scan heap
    buildstate.indtuples = 0;
    buildstate.heapRel = heap;

    reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
                                       hashbuildCallback, (void *) &buildstate, NULL);

    // Process spooled tuples if sorting was used
    if (buildstate.spool) {
        _h_indexbuild(buildstate.spool, buildstate.heapRel);
        _h_spooldestroy(buildstate.spool);
    }

    // Return build statistics
    result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;
    return result;
}
```