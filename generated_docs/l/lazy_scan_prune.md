# lazy_scan_prune

## Location
[src/backend/access/heap/vacuumlazy.c:1410-1653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L1410-L1653)

## Overview
Performs heap page pruning and freezing during VACUUM operations, handling HOT chain pruning, tuple freezing, dead tuple collection, and visibility map updates.

## Definition
```c
static int lazy_scan_prune(LVRelState *vacrel,
                          Buffer buf,
                          BlockNumber blkno,
                          Page page,
                          Buffer vmbuffer,
                          bool all_visible_according_to_vm,
                          bool *has_lpdead_items)
```

## Detailed Description
lazy_scan_prune is a core function in PostgreSQL's lazy VACUUM implementation that performs comprehensive heap page maintenance. It orchestrates the pruning of HOT (Heap-Only Tuple) update chains, freezes tuples when necessary, collects dead tuple information for index cleanup, and maintains visibility map consistency. The function handles the complex logic of determining page visibility status, managing the interaction between page-level and visibility map bits, and ensuring proper synchronization between heap pages and their corresponding visibility map entries. It also accumulates statistics about tuples processed and handles special cases like pages with LP_DEAD items that need index cleanup.

## Parameters / Member Variables
- `vacrel`: LVRelState containing VACUUM operation state and configuration
- `buf`: Buffer containing the heap page to process
- `blkno`: Block number of the page being processed
- `page`: Pointer to the actual page data
- `vmbuffer`: Buffer containing the visibility map block for this heap page
- `all_visible_according_to_vm`: Cached visibility status from earlier VM lookup
- `has_lpdead_items`: Output parameter indicating if LP_DEAD items remain on the page

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [heap_page_prune_and_freeze](../h/heap_page_prune_and_freeze.md)
  - [heap_page_is_all_visible](../h/heap_page_is_all_visible.md)
  - MultiXactIdIsValid
  - qsort
  - [cmpOffsetNumbers](../c/cmpOffsetNumbers.md)
  - [dead_items_add](../d/dead_items_add.md)
  - [PageSetAllVisible](../P/PageSetAllVisible.md)
  - [PageIsAllVisible](../P/PageIsAllVisible.md)
  - [PageClearAllVisible](../P/PageClearAllVisible.md)
  - [visibilitymap_set](../v/visibilitymap_set.md)
  - [visibilitymap_get_status](../v/visibilitymap_get_status.md)
  - [visibilitymap_clear](../v/visibilitymap_clear.md)
  - VM_ALL_FROZEN
- Called from:
  - [lazy_scan_heap](lazy_scan_heap.md)

## Notes and Other Information
- Returns the number of tuples deleted from the page during HOT pruning
- Handles complex visibility map synchronization with detailed error checking and warnings
- Sorts dead offsets using cmpOffsetNumbers before adding them to the dead items collection
- Updates various VACUUM statistics including frozen pages, dead items, and tuple counts
- Contains extensive assertion checking in debug builds to verify visibility map consistency
- Manages the relationship between page-level PD_ALL_VISIBLE bit and visibility map bits
- For relations without indexes, can immediately mark dead items as LP_UNUSED

## Simplified Source

```c
static int
lazy_scan_prune(LVRelState *vacrel,
                Buffer buf,
                BlockNumber blkno,
                Page page,
                Buffer vmbuffer,
                bool all_visible_according_to_vm,
                bool *has_lpdead_items)
{
    Relation rel = vacrel->rel;
    PruneFreezeResult presult;
    int prune_options = HEAP_PAGE_PRUNE_FREEZE;

    // For tables without indexes, can mark dead items unused immediately
    if (vacrel->nindexes == 0)
        prune_options |= HEAP_PAGE_PRUNE_MARK_UNUSED_NOW;

    // Perform actual pruning and freezing
    heap_page_prune_and_freeze(rel, buf, vacrel->vistest, prune_options,
                               &vacrel->cutoffs, &presult, PRUNE_VACUUM_SCAN,
                               &vacrel->offnum,
                               &vacrel->NewRelfrozenXid, &vacrel->NewRelminMxid);

    // Update frozen pages counter
    if (presult.nfrozen > 0)
        vacrel->frozen_pages++;

    // Collect LP_DEAD items for index cleanup
    if (presult.lpdead_items > 0)
    {
        vacrel->lpdead_item_pages++;

        // Sort dead offsets as required by dead_items_add
        qsort(presult.deadoffsets, presult.lpdead_items, sizeof(OffsetNumber),
              cmpOffsetNumbers);

        dead_items_add(vacrel, blkno, presult.deadoffsets, presult.lpdead_items);
    }

    // Update VACUUM statistics
    vacrel->tuples_deleted += presult.ndeleted;
    vacrel->tuples_frozen += presult.nfrozen;
    vacrel->lpdead_items += presult.lpdead_items;
    vacrel->live_tuples += presult.live_tuples;
    vacrel->recently_dead_tuples += presult.recently_dead_tuples;

    // Update truncation boundary
    if (presult.hastup)
        vacrel->nonempty_pages = blkno + 1;

    *has_lpdead_items = (presult.lpdead_items > 0);

    // Update visibility map if page became all-visible
    if (!all_visible_according_to_vm && presult.all_visible)
    {
        uint8 flags = VISIBILITYMAP_ALL_VISIBLE;
        if (presult.all_frozen)
            flags |= VISIBILITYMAP_ALL_FROZEN;

        PageSetAllVisible(page);
        MarkBufferDirty(buf);
        visibilitymap_set(vacrel->rel, blkno, buf, InvalidXLogRecPtr,
                         vmbuffer, presult.vm_conflict_horizon, flags);
    }
    // Handle visibility map inconsistencies
    else if (all_visible_according_to_vm && !PageIsAllVisible(page) &&
             visibilitymap_get_status(vacrel->rel, blkno, &vmbuffer) != 0)
    {
        elog(WARNING, "page is not marked all-visible but visibility map bit is set");
        visibilitymap_clear(vacrel->rel, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS);
    }

    return presult.ndeleted;
}
```