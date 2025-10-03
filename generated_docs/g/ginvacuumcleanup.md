# ginvacuumcleanup

## Location
[src/backend/access/gin/ginvacuum.c:688-801](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginvacuum.c#L688-L801)

## Overview
The vacuum cleanup function for GIN indexes that performs post-vacuum maintenance including statistics updates, free space management, and cleanup of pending insertions.

## Definition
```c
IndexBulkDeleteResult *ginvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
```

## Detailed Description
This function performs the cleanup phase of GIN index vacuum operations. It handles different scenarios based on the vacuum context: for analyze-only operations, it only cleans up pending insertions; for full vacuum operations, it performs comprehensive maintenance including page scanning, statistics collection, and free space management.

The function scans all pages in the index to identify recyclable pages, count different page types (data pages, entry pages), and collect accurate statistics. It updates the index metapage with current statistics, manages the free space map, and handles pending insertions that may have accumulated since the last vacuum. The function also properly handles locking requirements for relation extension to ensure consistent page counts.

## Parameters / Member Variables
- `info`: IndexVacuumInfo structure containing vacuum context including analyze_only flag and strategy
- `stats`: Existing IndexBulkDeleteResult from previous operations, or NULL for initial setup

## Dependencies
- Functions called/Symbols referenced:
  - AmAutoVacuumWorkerProcess
  - [initGinState](../i/initGinState.md)
  - [ginInsertCleanup](ginInsertCleanup.md)
  - [LockRelationForExtension](../L/LockRelationForExtension.md)
  - RelationGetNumberOfBlocks
  - [UnlockRelationForExtension](../U/UnlockRelationForExtension.md)
  - [ReadBufferExtended](../R/ReadBufferExtended.md)
  - [GinPageIsRecyclable](../G/GinPageIsRecyclable.md)
  - [RecordFreeIndexPage](../R/RecordFreeIndexPage.md)
  - GinPageIsData
  - GinPageIsList
  - GinPageIsLeaf
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [ginUpdateStats](ginUpdateStats.md)
  - [IndexFreeSpaceMapVacuum](../I/IndexFreeSpaceMapVacuum.md)
- Called from (representative examples):
  - [ginhandler](ginhandler.md) (as part of index AM interface)

## Notes and Other Information
- Entry point for GIN index vacuum cleanup called by the vacuum subsystem
- Handles both analyze-only and full vacuum scenarios differently
- For analyze-only in autovacuum context, only cleans up pending insertions
- Scans entire index to collect accurate page and entry statistics
- Identifies and records recyclable pages for future reuse
- Updates index metapage with current statistics about pages and entries
- Manages free space map to track available space for future insertions
- Uses appropriate locking to ensure consistent relation size measurements
- Includes vacuum delay points to avoid monopolizing system resources
- Reports heap tuple count as index entry count (limitation for partial indexes)
- Part of the PostgreSQL access method interface for GIN indexes
- Ensures pending insertions are processed even when ginbulkdelete wasn't called

## Simplified Source

```c
IndexBulkDeleteResult *
ginvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    Relation index = info->index;
    bool needLock;
    BlockNumber npages, blkno;
    BlockNumber totFreePages;
    GinState ginstate;
    GinStatsData idxStat;

    // For analyze-only operations, just clean up pending inserts
    if (info->analyze_only) {
        if (AmAutoVacuumWorkerProcess()) {
            initGinState(&ginstate, index);
            ginInsertCleanup(&ginstate, false, true, true, stats);
        }
        return stats;
    }

    // Initialize stats if ginbulkdelete wasn't called
    if (stats == NULL) {
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
        initGinState(&ginstate, index);
        ginInsertCleanup(&ginstate, !AmAutoVacuumWorkerProcess(),
                        false, true, stats);
    }

    memset(&idxStat, 0, sizeof(idxStat));

    // Set basic statistics
    stats->num_index_tuples = Max(info->num_heap_tuples, 0);
    stats->estimated_count = info->estimated_count;

    // Get relation size with appropriate locking
    needLock = !RELATION_IS_LOCAL(index);
    if (needLock)
        LockRelationForExtension(index, ExclusiveLock);
    npages = RelationGetNumberOfBlocks(index);
    if (needLock)
        UnlockRelationForExtension(index, ExclusiveLock);

    totFreePages = 0;

    // Scan all pages to collect statistics
    for (blkno = GIN_ROOT_BLKNO; blkno < npages; blkno++) {
        Buffer buffer;
        Page page;

        vacuum_delay_point();

        buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
                                   RBM_NORMAL, info->strategy);
        LockBuffer(buffer, GIN_SHARE);
        page = (Page) BufferGetPage(buffer);

        if (GinPageIsRecyclable(page)) {
            RecordFreeIndexPage(index, blkno);
            totFreePages++;
        } else if (GinPageIsData(page)) {
            idxStat.nDataPages++;
        } else if (!GinPageIsList(page)) {
            idxStat.nEntryPages++;
            if (GinPageIsLeaf(page))
                idxStat.nEntries += PageGetMaxOffsetNumber(page);
        }

        UnlockReleaseBuffer(buffer);
    }

    // Update metapage with collected statistics
    idxStat.nTotalPages = npages;
    ginUpdateStats(info->index, &idxStat, false);

    // Vacuum the free space map
    IndexFreeSpaceMapVacuum(info->index);

    // Set final statistics
    stats->pages_free = totFreePages;

    if (needLock)
        LockRelationForExtension(index, ExclusiveLock);
    stats->num_pages = RelationGetNumberOfBlocks(index);
    if (needLock)
        UnlockRelationForExtension(index, ExclusiveLock);

    return stats;
}
```