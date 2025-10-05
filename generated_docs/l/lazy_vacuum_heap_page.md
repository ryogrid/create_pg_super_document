# lazy_vacuum_heap_page

## Location
[src/backend/access/heap/vacuumlazy.c:2195-2299](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L2195-L2299)

## Overview
Processes a single heap page during vacuum by converting specified LP_DEAD items to LP_UNUSED, attempting line pointer array truncation, and updating visibility map information.

## Definition

```c
static void
lazy_vacuum_heap_page(LVRelState *vacrel, BlockNumber blkno, Buffer buffer,
					  OffsetNumber *deadoffsets, int num_offsets,
					  Buffer vmbuffer)
```
## Detailed Description
This function performs the actual heap page cleanup during the vacuum process. It takes a list of dead item offsets on a specific page and converts those LP_DEAD line pointers to LP_UNUSED, making the space available for reuse. The function operates within a critical section to ensure atomicity of the page modifications.

After marking items as unused, it attempts to truncate the line pointer array if there are contiguous unused items at the end, which helps reduce page overhead. The function also logs the changes to WAL if needed and updates the visibility map if the page becomes all-visible or all-frozen after the cleanup.

The function carefully manages the critical section to avoid doing complex operations (like visibility tests) while holding exclusive locks, which could lead to deadlocks or performance issues.

## Parameters / Member Variables
- `*vacrel`: LVRelState structure containing vacuum operation state and relation information
- `blkno`: Block number of the heap page being processed
- `buffer`: Buffer containing the heap page, must be exclusively locked by caller
- `*deadoffsets`: Array of offset numbers for LP_DEAD items to be marked as LP_UNUSED
- `num_offsets`: Number of offsets in the deadoffsets array
- `vmbuffer`: Buffer for the visibility map page, must be pinned by caller
## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - [update_vacuum_error_info](../u/update_vacuum_error_info.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsDead
  - ItemIdHasStorage  
  - ItemIdSetUnused
  - [PageTruncateLinePointerArray](../P/PageTruncateLinePointerArray.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - RelationNeedsWAL
  - [log_heap_prune_and_freeze](log_heap_prune_and_freeze.md)
  - [heap_page_is_all_visible](../h/heap_page_is_all_visible.md)
  - [PageSetAllVisible](../P/PageSetAllVisible.md)
  - [visibilitymap_set](../v/visibilitymap_set.md)
  - [restore_vacuum_error_info](../r/restore_vacuum_error_info.md)
- Called from:
  - [lazy_vacuum_heap_rel](lazy_vacuum_heap_rel.md)

## Notes and Other Information
- Requires caller to hold exclusive buffer lock (cleanup lock also acceptable)
- Requires vmbuffer to be valid and pinned on the visibility map page for blkno
- Only processes pages when do_index_vacuuming is enabled
- Updates progress reporting with PROGRESS_VACUUM_HEAP_BLKS_VACUUMED
- Uses critical sections around page modifications to ensure atomicity
- Attempts to set page as all-visible/all-frozen after cleanup if conditions are met
- Logs WAL record with PRUNE_VACUUM_CLEANUP reason when relation needs WAL
- Includes assertions to verify that processed items are actually LP_DEAD without storage

## Simplified Source

```c
static void
lazy_vacuum_heap_page(LVRelState *vacrel, BlockNumber blkno, Buffer buffer,
                      OffsetNumber *deadoffsets, int num_offsets,
                      Buffer vmbuffer)
{
    Page page = BufferGetPage(buffer);
    OffsetNumber unused[MaxHeapTuplesPerPage];
    int nunused = 0;
    TransactionId visibility_cutoff_xid;
    bool all_frozen;
    LVSavedErrInfo saved_err_info;

    Assert(vacrel->do_index_vacuuming);

    pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, blkno);

    // Update error traceback
    update_vacuum_error_info(vacrel, &saved_err_info,
                            VACUUM_ERRCB_PHASE_VACUUM_HEAP, blkno,
                            InvalidOffsetNumber);

    START_CRIT_SECTION();

    // Mark all dead items as unused
    for (int i = 0; i < num_offsets; i++)
    {
        OffsetNumber toff = deadoffsets[i];
        ItemId itemid = PageGetItemId(page, toff);

        Assert(ItemIdIsDead(itemid) && !ItemIdHasStorage(itemid));
        ItemIdSetUnused(itemid);
        unused[nunused++] = toff;
    }

    Assert(nunused > 0);

    // Try to truncate line pointer array
    PageTruncateLinePointerArray(page);

    // Mark buffer dirty and log to WAL
    MarkBufferDirty(buffer);

    if (RelationNeedsWAL(vacrel->rel))
    {
        log_heap_prune_and_freeze(vacrel->rel, buffer,
                                 InvalidTransactionId,
                                 false, /* no cleanup lock required */
                                 PRUNE_VACUUM_CLEANUP,
                                 NULL, 0, /* frozen */
                                 NULL, 0, /* redirected */
                                 NULL, 0, /* dead */
                                 unused, nunused);
    }

    END_CRIT_SECTION();

    // Check if page became all-visible after cleanup
    Assert(!PageIsAllVisible(page));
    if (heap_page_is_all_visible(vacrel, buffer, &visibility_cutoff_xid, &all_frozen))
    {
        uint8 flags = VISIBILITYMAP_ALL_VISIBLE;

        if (all_frozen)
        {
            Assert(!TransactionIdIsValid(visibility_cutoff_xid));
            flags |= VISIBILITYMAP_ALL_FROZEN;
        }

        PageSetAllVisible(page);
        visibilitymap_set(vacrel->rel, blkno, buffer, InvalidXLogRecPtr,
                         vmbuffer, visibility_cutoff_xid, flags);
    }

    // Restore error info
    restore_vacuum_error_info(vacrel, &saved_err_info);
}
```