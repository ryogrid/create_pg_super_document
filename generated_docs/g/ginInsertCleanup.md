# ginInsertCleanup

## Location
[src/backend/access/gin/ginfast.c:780-1030](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginfast.c#L780-L1030)

## Overview
Moves tuples from pending pages into the regular GIN index structure, handling the transition from fast insertion to the main index with crash-safe cleanup processing.

## Definition
```c
void ginInsertCleanup(GinState *ginstate, bool full_clean, bool fill_fsm, bool forceCleanup, IndexBulkDeleteResult *stats)
```

## Detailed Description
This function performs the critical task of transferring entries from the GIN pending list to the main index structure. It operates in a crash-safe manner by ensuring that duplicate entries in the main index are harmless if a crash occurs mid-process. The function uses exclusive locking on the metapage to prevent concurrent cleanup while allowing concurrent insertions. It processes pages in batches to manage memory usage and can perform either partial or complete cleanup of the pending list.

## Parameters / Member Variables
- `ginstate`: Pointer to GinState structure containing index information and configuration
- `full_clean`: Boolean indicating whether to clean the entire pending list or stop at the remembered tail
- `fill_fsm`: Boolean indicating whether ginInsertCleanup should add deleted pages to the Free Space Map
- `forceCleanup`: Boolean indicating whether to wait for concurrent cleanup (from vacuum/analyze) or exit immediately
- `stats`: Pointer to IndexBulkDeleteResult for counting deleted pending pages (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [LockPage](../L/LockPage.md), ConditionalLockPage, UnlockPage (page locking functions)
  - [ReadBuffer](../R/ReadBuffer.md), LockBuffer, UnlockReleaseBuffer (buffer management)
  - GinPageGetMeta, GinPageGetOpaque (GIN page access functions)
  - AllocSetContextCreate, MemoryContextSwitchTo, MemoryContextReset (memory management)
  - [initKeyArray](../i/initKeyArray.md), ginInitBA (initialization functions)
  - [processPendingPage](../p/processPendingPage.md) (process individual pending pages)
  - [ginBeginBAScan](ginBeginBAScan.md), ginGetBAEntry (build accumulator scanning)
  - [ginEntryInsert](ginEntryInsert.md) (insert entries into main index)
  - [shiftList](../s/shiftList.md) (remove processed pages from pending list)
  - [IndexFreeSpaceMapVacuum](../I/IndexFreeSpaceMapVacuum.md) (FSM maintenance)
- Called from (representative examples):
  - [ginHeapTupleFastInsert](ginHeapTupleFastInsert.md) (at src/backend/access/gin/ginfast.c:471)
  - [gin_clean_pending_list](gin_clean_pending_list.md) (at src/backend/access/gin/ginfast.c:1080)
  - [ginbulkdelete](ginbulkdelete.md) (at src/backend/access/gin/ginvacuum.c:593)
  - [ginvacuumcleanup](ginvacuumcleanup.md) (at src/backend/access/gin/ginvacuum.c:707, 720)

## Notes and Other Information
- This is a public function accessible from other GIN modules
- Uses different work memory limits based on whether it's called from autovacuum or regular operations
- Implements a crash-safe cleanup mechanism where duplicate entries are harmless
- Prevents infinite cleanup when other backends add tuples faster than cleanup by remembering the tail page
- Manages memory usage by flushing to disk when memory limits are reached or at page boundaries
- Supports both forced cleanup (waits for concurrent cleanup) and opportunistic cleanup (exits if concurrent cleanup is running)
- Handles concurrent insertions by processing any new entries added while pages were unlocked
- Uses vacuum delay points to prevent monopolizing system resources
- Can trigger Free Space Map vacuum for efficient space reclamation
- Critical component of the GIN fast insertion mechanism

## Simplified Source

```c
// Simplified version of ginInsertCleanup
void ginInsertCleanup(GinState *ginstate, bool full_clean,
                     bool fill_fsm, bool forceCleanup,
                     IndexBulkDeleteResult *stats)
{
    Relation index = ginstate->index;
    Buffer metabuffer, buffer;
    Page metapage, page;
    GinMetaPageData *metadata;
    MemoryContext opCtx, oldCtx;
    BuildAccumulator accum;
    KeyArray datums;
    BlockNumber blkno, blknoFinish;
    bool cleanupFinish = false;
    bool fsm_vac = false;
    Size workMemory;

    // Acquire exclusive lock on metapage
    if (forceCleanup) {
        LockPage(index, GIN_METAPAGE_BLKNO, ExclusiveLock);
        workMemory = (AmAutoVacuumWorkerProcess() && autovacuum_work_mem != -1) ?
                     autovacuum_work_mem : maintenance_work_mem;
    } else {
        if (!ConditionalLockPage(index, GIN_METAPAGE_BLKNO, ExclusiveLock))
            return;
        workMemory = work_mem;
    }

    // Check if there's anything to clean
    metabuffer = ReadBuffer(index, GIN_METAPAGE_BLKNO);
    LockBuffer(metabuffer, GIN_SHARE);
    metapage = BufferGetPage(metabuffer);
    metadata = GinPageGetMeta(metapage);

    if (metadata->head == InvalidBlockNumber) {
        UnlockReleaseBuffer(metabuffer);
        UnlockPage(index, GIN_METAPAGE_BLKNO, ExclusiveLock);
        return;
    }

    // Remember tail to prevent infinite cleanup
    blknoFinish = metadata->tail;
    blkno = metadata->head;
    buffer = ReadBuffer(index, blkno);
    LockBuffer(buffer, GIN_SHARE);
    page = BufferGetPage(buffer);
    LockBuffer(metabuffer, GIN_UNLOCK);

    // Initialize temporary context and accumulators
    opCtx = AllocSetContextCreate(CurrentMemoryContext,
                                 "GIN insert cleanup temporary context",
                                 ALLOCSET_DEFAULT_SIZES);
    oldCtx = MemoryContextSwitchTo(opCtx);
    initKeyArray(&datums, 128);
    ginInitBA(&accum);
    accum.ginstate = ginstate;

    // Main cleanup loop
    for (;;) {
        // Check if we've reached the finish point
        if (blkno == blknoFinish && full_clean == false)
            cleanupFinish = true;

        // Process current page
        processPendingPage(&accum, &datums, page, FirstOffsetNumber);
        vacuum_delay_point();

        // Check if it's time to flush to main index
        if (GinPageGetOpaque(page)->rightlink == InvalidBlockNumber ||
            (GinPageHasFullRow(page) &&
             (accum.allocatedMemory >= workMemory * 1024L))) {

            ItemPointerData *list;
            uint32 nlist;
            Datum key;
            GinNullCategory category;
            OffsetNumber maxoff, attnum;

            // Unlock page during expensive operations
            maxoff = PageGetMaxOffsetNumber(page);
            LockBuffer(buffer, GIN_UNLOCK);

            // Insert accumulated entries into main index
            ginBeginBAScan(&accum);
            while ((list = ginGetBAEntry(&accum, &attnum, &key, &category, &nlist)) != NULL) {
                ginEntryInsert(ginstate, attnum, key, category, list, nlist, NULL);
                vacuum_delay_point();
            }

            // Re-lock and handle concurrent insertions
            LockBuffer(metabuffer, GIN_EXCLUSIVE);
            LockBuffer(buffer, GIN_SHARE);

            if (PageGetMaxOffsetNumber(page) != maxoff) {
                // Process new entries added while unlocked
                ginInitBA(&accum);
                processPendingPage(&accum, &datums, page, maxoff + 1);
                ginBeginBAScan(&accum);
                while ((list = ginGetBAEntry(&accum, &attnum, &key, &category, &nlist)) != NULL)
                    ginEntryInsert(ginstate, attnum, key, category, list, nlist, NULL);
            }

            // Remove processed pages from pending list
            blkno = GinPageGetOpaque(page)->rightlink;
            UnlockReleaseBuffer(buffer);
            shiftList(index, metabuffer, blkno, fill_fsm, stats);
            fsm_vac = true;

            LockBuffer(metabuffer, GIN_UNLOCK);

            // Check if cleanup is complete
            if (blkno == InvalidBlockNumber || cleanupFinish)
                break;

            // Reset for next batch
            MemoryContextReset(opCtx);
            initKeyArray(&datums, datums.maxvalues);
            ginInitBA(&accum);
        } else {
            blkno = GinPageGetOpaque(page)->rightlink;
            UnlockReleaseBuffer(buffer);
        }

        // Read next page
        vacuum_delay_point();
        buffer = ReadBuffer(index, blkno);
        LockBuffer(buffer, GIN_SHARE);
        page = BufferGetPage(buffer);
    }

    UnlockPage(index, GIN_METAPAGE_BLKNO, ExclusiveLock);
    ReleaseBuffer(metabuffer);

    // Trigger FSM vacuum if pages were freed
    if (fsm_vac && fill_fsm)
        IndexFreeSpaceMapVacuum(index);

    // Clean up temporary context
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(opCtx);
}
```