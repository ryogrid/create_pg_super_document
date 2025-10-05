# lazy_scan_noprune

## Location
[src/backend/access/heap/vacuumlazy.c:1654-1864](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L1654-L1864)

## Overview
A lightweight heap page scanning function that processes pages without performing pruning or freezing, collecting dead items and statistics while requiring only a shared buffer lock.

## Definition
```c
static bool lazy_scan_noprune(LVRelState *vacrel,
                             Buffer buf,
                             BlockNumber blkno,
                             Page page,
                             bool *has_lpdead_items)
```

## Detailed Description
lazy_scan_noprune is an optimized variant of lazy_scan_prune that processes heap pages when a full cleanup lock cannot be obtained or is not needed. It scans all tuples on the page to collect statistics and identify LP_DEAD items left by previous pruning operations, but does not perform any actual pruning or freezing. The function serves as a fallback when VACUUM cannot get exclusive access to a page, allowing it to still gather useful information and handle LP_DEAD items for index cleanup. For aggressive VACUUMs, it may return false to indicate that full processing with lazy_scan_prune is required when tuple freezing is necessary to advance the relation's freeze parameters.

## Parameters / Member Variables
- `vacrel`: LVRelState containing VACUUM operation state and configuration
- `buf`: Buffer containing the heap page to process (shared lock sufficient)
- `blkno`: Block number of the page being processed
- `page`: Pointer to the actual page data
- `has_lpdead_items`: Output parameter indicating if LP_DEAD items were found on the page

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsUsed
  - ItemIdIsRedirected
  - ItemIdIsDead
  - [PageGetItem](../P/PageGetItem.md)
  - [heap_tuple_should_freeze](../h/heap_tuple_should_freeze.md)
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - ItemIdGetLength
  - [HeapTupleSatisfiesVacuum](../H/HeapTupleSatisfiesVacuum.md)
  - [dead_items_add](../d/dead_items_add.md)
- Called from:
  - [lazy_scan_heap](lazy_scan_heap.md)

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsUsed
  - ItemIdIsRedirected
  - ItemIdIsDead
  - [PageGetItem](../P/PageGetItem.md)
  - [heap_tuple_should_freeze](../h/heap_tuple_should_freeze.md)
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - ItemIdGetLength
  - [HeapTupleSatisfiesVacuum](../H/HeapTupleSatisfiesVacuum.md)
  - [dead_items_add](../d/dead_items_add.md)
- Called from:
  - [lazy_scan_heap](lazy_scan_heap.md)

## Notes and Other Information
- Returns true if processing completed successfully, false if aggressive VACUUM needs full pruning
- Requires only shared buffer lock unlike lazy_scan_prune which needs cleanup lock
- For aggressive VACUUMs, returns false when tuples need freezing to advance freeze horizons
- Handles special case of tables without indexes using single-pass strategy
- Counts various tuple states (live, recently dead, missed dead) for VACUUM statistics
- Does not modify the page content, only collects information and LP_DEAD items
- Updates vacrel statistics including live_tuples, recently_dead_tuples, and missed_dead_tuples

## Simplified Source

```c
static bool
lazy_scan_noprune(LVRelState *vacrel,
                  Buffer buf,
                  BlockNumber blkno,
                  Page page,
                  bool *has_lpdead_items)
{
    OffsetNumber offnum, maxoff;
    int lpdead_items = 0, live_tuples = 0, recently_dead_tuples = 0, missed_dead_tuples = 0;
    bool hastup = false;
    OffsetNumber deadoffsets[MaxHeapTuplesPerPage];
    TransactionId NoFreezePageRelfrozenXid = vacrel->NewRelfrozenXid;
    MultiXactId NoFreezePageRelminMxid = vacrel->NewRelminMxid;

    // Scan all line pointers on the page
    maxoff = PageGetMaxOffsetNumber(page);
    for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum))
    {
        ItemId itemid = PageGetItemId(page, offnum);
        HeapTupleData tuple;
        HeapTupleHeader tupleheader;

        vacrel->offnum = offnum;

        if (!ItemIdIsUsed(itemid))
            continue;

        if (ItemIdIsRedirected(itemid))
        {
            hastup = true;
            continue;
        }

        if (ItemIdIsDead(itemid))
        {
            deadoffsets[lpdead_items++] = offnum;
            continue;
        }

        hastup = true;
        tupleheader = (HeapTupleHeader) PageGetItem(page, itemid);

        // Check if tuple needs freezing
        if (heap_tuple_should_freeze(tupleheader, &vacrel->cutoffs,
                                   &NoFreezePageRelfrozenXid, &NoFreezePageRelminMxid))
        {
            // Aggressive VACUUM requires freezing - must use lazy_scan_prune
            if (vacrel->aggressive)
            {
                vacrel->offnum = InvalidOffsetNumber;
                return false; // Caller must use lazy_scan_prune
            }
            // Non-aggressive VACUUM can skip freezing
        }

        // Check tuple visibility status
        ItemPointerSet(&(tuple.t_self), blkno, offnum);
        tuple.t_data = tupleheader;
        tuple.t_len = ItemIdGetLength(itemid);
        tuple.t_tableOid = RelationGetRelid(vacrel->rel);

        switch (HeapTupleSatisfiesVacuum(&tuple, vacrel->cutoffs.OldestXmin, buf))
        {
            case HEAPTUPLE_DELETE_IN_PROGRESS:
            case HEAPTUPLE_LIVE:
                live_tuples++;
                break;
            case HEAPTUPLE_DEAD:
                missed_dead_tuples++;
                break;
            case HEAPTUPLE_RECENTLY_DEAD:
                recently_dead_tuples++;
                break;
            case HEAPTUPLE_INSERT_IN_PROGRESS:
                break; // Don't count as live
            default:
                elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
                break;
        }
    }

    vacrel->offnum = InvalidOffsetNumber;
    vacrel->NewRelfrozenXid = NoFreezePageRelfrozenXid;
    vacrel->NewRelminMxid = NoFreezePageRelminMxid;

    // Handle LP_DEAD items
    if (vacrel->nindexes == 0)
    {
        // Single-pass strategy - count LP_DEAD as missed
        if (lpdead_items > 0)
        {
            hastup = true;
            missed_dead_tuples += lpdead_items;
        }
    }
    else if (lpdead_items > 0)
    {
        // Multi-pass strategy - collect LP_DEAD items for index cleanup
        vacrel->lpdead_item_pages++;
        dead_items_add(vacrel, blkno, deadoffsets, lpdead_items);
        vacrel->lpdead_items += lpdead_items;
    }

    // Update VACUUM statistics
    vacrel->live_tuples += live_tuples;
    vacrel->recently_dead_tuples += recently_dead_tuples;
    vacrel->missed_dead_tuples += missed_dead_tuples;
    if (missed_dead_tuples > 0)
        vacrel->missed_dead_pages++;

    // Update truncation boundary
    if (hastup)
        vacrel->nonempty_pages = blkno + 1;

    *has_lpdead_items = (lpdead_items > 0);

    return true; // Processing completed successfully
}
```