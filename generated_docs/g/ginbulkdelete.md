# ginbulkdelete

## Location
[src/backend/access/gin/ginvacuum.c:565-687](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginvacuum.c#L565-L687)

## Overview
The main bulk delete function for GIN indexes that performs vacuum operations by traversing entry pages and posting trees to remove dead tuples and reclaim storage space.

## Definition
```c
IndexBulkDeleteResult *ginbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                     IndexBulkDeleteCallback callback, void *callback_state)
```

## Detailed Description
This function implements the bulk delete operation for GIN (Generalized Inverted) indexes during vacuum operations. It performs a complete traversal of the index structure, starting from the root and descending to leaf pages. The function processes entry pages to remove dead tuples from posting lists and collects posting tree roots for deferred processing.

The algorithm follows a two-phase approach: first it processes all entry pages sequentially from left to right, then it processes any posting trees found during the entry page scan. This ordering prevents deadlocks that could occur if posting trees were processed immediately during the entry page scan. The function also handles cleanup of pending inserts and maintains vacuum statistics throughout the operation.

## Parameters / Member Variables
- `info`: IndexVacuumInfo structure containing vacuum context including target index and strategy
- `stats`: Existing IndexBulkDeleteResult for accumulating statistics, or NULL for first-time execution
- `callback`: Function pointer to determine if specific tuples should be deleted
- `callback_state`: Opaque state data passed to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [initGinState](../i/initGinState.md)
  - [ginInsertCleanup](ginInsertCleanup.md)
  - [ReadBufferExtended](../R/ReadBufferExtended.md)
  - [ginVacuumEntryPage](ginVacuumEntryPage.md)
  - [ginVacuumPostingTree](ginVacuumPostingTree.md)
  - [PageRestoreTempPage](../P/PageRestoreTempPage.md)
  - [xlogVacuumPage](../x/xlogVacuumPage.md)
  - [vacuum_delay_point](../v/vacuum_delay_point.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [ginhandler](ginhandler.md) (as part of index AM interface)

## Notes and Other Information
- Entry point for GIN index vacuum operations called by the vacuum subsystem
- Creates temporary memory context for vacuum operations to manage memory usage
- Processes entry pages sequentially from leftmost to rightmost leaf page
- Defers posting tree processing to avoid deadlock risks during concurrent operations
- Uses critical sections around page modifications to ensure crash safety
- Includes vacuum delay points to avoid monopolizing system resources
- Maintains comprehensive statistics about tuples processed and pages modified
- Handles both initial vacuum runs (stats == NULL) and subsequent runs
- Properly cleans up pending inserts before beginning main vacuum work
- Part of the PostgreSQL access method interface for GIN indexes

## Simplified Source

```c
IndexBulkDeleteResult *
ginbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
              IndexBulkDeleteCallback callback, void *callback_state)
{
    Relation index = info->index;
    BlockNumber blkno = GIN_ROOT_BLKNO;
    GinVacuumState gvs;
    Buffer buffer;
    BlockNumber rootOfPostingTree[BLCKSZ / (sizeof(IndexTupleData) + sizeof(ItemId))];
    uint32 nRoot;

    // Setup vacuum state
    gvs.tmpCxt = AllocSetContextCreate(CurrentMemoryContext,
                                      "Gin vacuum temporary context",
                                      ALLOCSET_DEFAULT_SIZES);
    gvs.index = index;
    gvs.callback = callback;
    gvs.callback_state = callback_state;
    gvs.strategy = info->strategy;
    initGinState(&gvs.ginstate, index);

    // Initialize stats on first run
    if (stats == NULL) {
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
        ginInsertCleanup(&gvs.ginstate, !AmAutoVacuumWorkerProcess(),
                        false, true, stats);
    }

    stats->num_index_tuples = 0;
    gvs.result = stats;

    // Navigate to leftmost leaf page
    buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
                               RBM_NORMAL, info->strategy);

    while (true) {
        Page page = BufferGetPage(buffer);
        IndexTuple itup;

        LockBuffer(buffer, GIN_SHARE);

        if (GinPageIsLeaf(page)) {
            LockBuffer(buffer, GIN_UNLOCK);
            LockBuffer(buffer, GIN_EXCLUSIVE);

            // Handle root page concurrency
            if (blkno == GIN_ROOT_BLKNO && !GinPageIsLeaf(page)) {
                LockBuffer(buffer, GIN_UNLOCK);
                continue;
            }
            break;
        }

        // Descend to next level
        itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
        blkno = GinGetDownlink(itup);

        UnlockReleaseBuffer(buffer);
        buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
                                   RBM_NORMAL, info->strategy);
    }

    // Process all entry pages from left to right
    while (true) {
        Page page = BufferGetPage(buffer);
        Page resPage;
        uint32 i;

        // Vacuum the entry page
        resPage = ginVacuumEntryPage(&gvs, buffer, rootOfPostingTree, &nRoot);

        blkno = GinPageGetOpaque(page)->rightlink;

        // Apply changes if page was modified
        if (resPage) {
            START_CRIT_SECTION();
            PageRestoreTempPage(resPage, page);
            MarkBufferDirty(buffer);
            xlogVacuumPage(gvs.index, buffer);
            UnlockReleaseBuffer(buffer);
            END_CRIT_SECTION();
        } else {
            UnlockReleaseBuffer(buffer);
        }

        vacuum_delay_point();

        // Process any posting trees found on this page
        for (i = 0; i < nRoot; i++) {
            ginVacuumPostingTree(&gvs, rootOfPostingTree[i]);
            vacuum_delay_point();
        }

        // Move to next entry page or finish
        if (blkno == InvalidBlockNumber)
            break;

        buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
                                   RBM_NORMAL, info->strategy);
        LockBuffer(buffer, GIN_EXCLUSIVE);
    }

    MemoryContextDelete(gvs.tmpCxt);
    return gvs.result;
}
```