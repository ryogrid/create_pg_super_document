# btvacuumpage

## Location
[src/backend/access/nbtree/nbtree.c:1073-1407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtree.c#L1073-L1407)

## Overview
Processes a single page during B-tree vacuum operations, handling deletions, updates to posting list tuples, and page splits that occurred during the vacuum cycle.

## Definition

```c
static void
btvacuumpage(BTVacState *vstate, BlockNumber scanblkno)
```
## Detailed Description
The  function is the core page-processing routine for B-tree vacuum operations. It handles a single page identified by  during a vacuum scan. The function manages complex scenarios including:

1. **Page recycling**: Identifying and recycling deleted or empty pages
2. **Tuple deletion**: Removing dead tuples from leaf pages based on callback results
3. **Posting list updates**: Handling partial deletions in posting list tuples (where some TIDs are dead but others remain live)
4. **Page split handling**: Detecting and handling page splits that occurred after the vacuum cycle began by backtracking to process sibling pages
5. **Half-dead page cleanup**: Finishing deletion of pages left in half-dead state by interrupted vacuum operations

The function implements a sophisticated backtracking mechanism to ensure that page splits occurring during the vacuum don't cause tuples to be missed. When a page split moves tuples to a block number lower than the current scan position, the function backtracks to process those pages.

## Parameters / Member Variables
- `*vstate`: BTVacState structure containing vacuum state information including callback function, statistics, and cycle ID
- `scanblkno`: Block number of the page to vacuum, which may differ from the actual page being processed during backtracking
## Dependencies
- Functions called/Symbols referenced:
  - [vacuum_delay_point](../v/vacuum_delay_point.md)
  - [ReadBufferExtended](../R/ReadBufferExtended.md)
  - [_bt_lockbuf](_bt_lockbuf.md)/_bt_relbuf
  - [_bt_checkpage](_bt_checkpage.md)
  - BTPageGetOpaque
  - [BTPageIsRecyclable](../B/BTPageIsRecyclable.md)
  - [RecordFreeIndexPage](../R/RecordFreeIndexPage.md)
  - [_bt_upgradelockbufcleanup](_bt_upgradelockbufcleanup.md)
  - [btreevacuumposting](btreevacuumposting.md)
  - [_bt_delitems_vacuum](_bt_delitems_vacuum.md)
  - [_bt_pagedel](_bt_pagedel.md)
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md)/BTreeTupleIsPivot
  - [BTreeTupleGetNPosting](../B/BTreeTupleGetNPosting.md)
- Called from:
  - [btvacuumscan](btvacuumscan.md) (main vacuum scan function)

## Notes and Other Information
- Uses a 'goto backtrack' mechanism to handle page splits that occurred during vacuum
- Maintains detailed statistics about deleted tuples, pages, and TIDs
- Implements memory management using temporary contexts for page deletion operations
- Handles both regular tuples and posting list tuples (which contain multiple TIDs)
- Updates btpo_cycleid to prevent reprocessing of split pages
- Critical for maintaining B-tree index integrity during vacuum operations

## Simplified Source

```c
static void btvacuumpage(BTVacState *vstate, BlockNumber scanblkno) {
    IndexVacuumInfo *info = vstate->info;
    IndexBulkDeleteResult *stats = vstate->stats;
    IndexBulkDeleteCallback callback = vstate->callback;
    Relation rel = info->index;
    bool attempt_pagedel = false;
    BlockNumber blkno = scanblkno, backtrack_to = P_NONE;
    Buffer buf;
    Page page;
    BTPageOpaque opaque;

backtrack:
    attempt_pagedel = false;
    backtrack_to = P_NONE;

    vacuum_delay_point();

    // Read page with non-default strategy, handle all-zero pages
    buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, info->strategy);
    _bt_lockbuf(rel, buf, BT_READ);
    page = BufferGetPage(buf);

    if (!PageIsNew(page)) {
        _bt_checkpage(rel, buf);
        opaque = BTPageGetOpaque(page);
    } else {
        opaque = NULL;
    }

    // Handle backtracking for split pages
    if (blkno != scanblkno) {
        if (!opaque || !P_ISLEAF(opaque) || P_ISHALFDEAD(opaque) ||
            opaque->btpo_cycleid != vstate->cycleid || P_ISDELETED(opaque)) {
            _bt_relbuf(rel, buf);
            return;
        }
    }

    // Process different page types
    if (!opaque || BTPageIsRecyclable(page, info->heaprel)) {
        // Recyclable page
        RecordFreeIndexPage(rel, blkno);
        stats->pages_deleted++;
        stats->pages_free++;
    } else if (P_ISDELETED(opaque)) {
        // Already deleted page
        stats->pages_deleted++;
    } else if (P_ISHALFDEAD(opaque)) {
        // Half-dead page needs finishing
        attempt_pagedel = true;
    } else if (P_ISLEAF(opaque)) {
        // Process leaf page tuples
        OffsetNumber deletable[MaxIndexTuplesPerPage];
        int ndeletable = 0, nupdatable = 0;
        BTVacuumPosting updatable[MaxIndexTuplesPerPage];
        OffsetNumber minoff, maxoff;
        int nhtidsdead = 0, nhtidslive = 0;

        _bt_upgradelockbufcleanup(rel, buf);

        // Check for backtracking due to page splits
        if (vstate->cycleid != 0 && opaque->btpo_cycleid == vstate->cycleid &&
            !(opaque->btpo_flags & BTP_SPLIT_END) && !P_RIGHTMOST(opaque) &&
            opaque->btpo_next < scanblkno) {
            backtrack_to = opaque->btpo_next;
        }

        minoff = P_FIRSTDATAKEY(opaque);
        maxoff = PageGetMaxOffsetNumber(page);

        // Process tuples with callback if provided
        if (callback) {
            for (OffsetNumber offnum = minoff; offnum <= maxoff;
                 offnum = OffsetNumberNext(offnum)) {
                IndexTuple itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offnum));

                if (!BTreeTupleIsPosting(itup)) {
                    // Regular tuple
                    if (callback(&itup->t_tid, vstate->callback_state)) {
                        deletable[ndeletable++] = offnum;
                        nhtidsdead++;
                    } else {
                        nhtidslive++;
                    }
                } else {
                    // Posting list tuple
                    BTVacuumPosting vacposting;
                    int nremaining;

                    vacposting = btreevacuumposting(vstate, itup, offnum, &nremaining);
                    if (vacposting == NULL) {
                        // No changes needed
                    } else if (nremaining > 0) {
                        // Partial deletion - update tuple
                        updatable[nupdatable++] = vacposting;
                        nhtidsdead += BTreeTupleGetNPosting(itup) - nremaining;
                    } else {
                        // Complete deletion
                        deletable[ndeletable++] = offnum;
                        nhtidsdead += BTreeTupleGetNPosting(itup);
                        pfree(vacposting);
                    }
                    nhtidslive += nremaining;
                }
            }
        }

        // Apply deletions and updates
        if (ndeletable > 0 || nupdatable > 0) {
            _bt_delitems_vacuum(rel, buf, deletable, ndeletable, updatable, nupdatable);
            stats->tuples_removed += nhtidsdead;
            maxoff = PageGetMaxOffsetNumber(page);

            // Free memory for updatable items
            for (int i = 0; i < nupdatable; i++)
                pfree(updatable[i]);
        } else if (vstate->cycleid != 0 && opaque->btpo_cycleid == vstate->cycleid) {
            // Clear cycle ID to prevent reprocessing
            opaque->btpo_cycleid = 0;
            MarkBufferDirtyHint(buf, true);
        }

        // Update statistics and check for page deletion
        if (minoff > maxoff)
            attempt_pagedel = (blkno == scanblkno);
        else if (callback)
            stats->num_index_tuples += nhtidslive;
        else
            stats->num_index_tuples += maxoff - minoff + 1;
    }

    // Handle page deletion if needed
    if (attempt_pagedel) {
        MemoryContext oldcontext = MemoryContextSwitchTo(vstate->pagedelcontext);
        _bt_pagedel(rel, buf, vstate);
        MemoryContextSwitchTo(oldcontext);
        MemoryContextReset(vstate->pagedelcontext);
    } else {
        _bt_relbuf(rel, buf);
    }

    // Handle backtracking
    if (backtrack_to != P_NONE) {
        blkno = backtrack_to;
        goto backtrack;
    }
}
```