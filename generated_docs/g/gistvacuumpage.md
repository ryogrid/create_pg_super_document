# gistvacuumpage

## Location
[src/backend/access/gist/gistvacuum.c:272-460](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistvacuum.c#L272-L460)

## Overview
Processes a single page during GiST index vacuum operations, handling tuple deletion, page recycling, split detection, and maintaining vacuum state.

## Definition

```c
static void
gistvacuumpage(GistVacState *vstate, BlockNumber blkno, BlockNumber orig_blkno)
```
## Detailed Description
This function performs detailed processing of individual pages during GiST vacuum operations. It handles multiple scenarios: recyclable pages that can be immediately reused, deleted pages that need tracking, leaf pages requiring tuple-level processing, and internal pages that need structural validation.

For leaf pages, the function implements sophisticated logic to detect and handle concurrent page splits that might have occurred during the vacuum scan. When splits move tuples to lower-numbered pages that were already processed, the function sets up tail recursion to revisit those pages.

The function performs tuple-level deletion on leaf pages using provided callback criteria, batching deletions for efficiency and generating appropriate WAL records. It also detects completely empty pages for later removal and maintains accurate tuple counts for statistics.

For internal pages, it validates tuple integrity and detects legacy "invalid tuples" from PostgreSQL versions prior to 9.1, providing diagnostic messages when encountered.

## Parameters / Member Variables
- `*vstate`: GistVacState structure containing vacuum context, statistics, page sets, and callback information
- `blkno`: Block number of the page currently being processed
- `orig_blkno`: Highest block number reached by the outer scan (used for split detection and recursion control)
## Dependencies
- Functions called/Symbols referenced:
  - [vacuum_delay_point](../v/vacuum_delay_point.md) (vacuum throttling)
  - [ReadBufferExtended](../R/ReadBufferExtended.md), LockBuffer, BufferGetPage, UnlockReleaseBuffer (buffer management)
  - [gistPageRecyclable](gistPageRecyclable.md), GistPageIsDeleted, GistPageIsLeaf (page state checking)
  - [RecordFreeIndexPage](../R/RecordFreeIndexPage.md) (FSM management)
  - GistFollowRight, GistPageGetNSN (split detection logic)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md), PageGetItemId, PageGetItem (page/tuple access)
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md), GistMarkTuplesDeleted (tuple deletion)
  - [gistXLogUpdate](gistXLogUpdate.md), gistGetFakeLSN (WAL logging)
  - [intset_add_member](../i/intset_add_member.md) (page set tracking for internal and empty pages)
  - GistTupleIsInvalid (legacy tuple validation)
- Called from (representative examples):
  - [gistvacuumscan](gistvacuumscan.md) (main vacuum scanning loop)

## Notes and Other Information
- Uses tail recursion optimization (implemented as a goto loop) to handle concurrent page splits efficiently
- Implements aggressive exclusive locking strategy since processing time per page is expected to be short
- Generates single WAL record per page for all tuple deletions to minimize WAL traffic
- Maintains separate tracking of internal pages and empty leaf pages using integer sets
- Handles page splits that occurred during vacuum by checking NSN (Node Sequence Number) and rightlink pointers
- Detects and reports legacy invalid tuples from pre-9.1 PostgreSQL versions with detailed diagnostic information
- Only adds pages to tracking sets when blkno == orig_blkno to maintain ascending order requirement for IntegerSet
- The function is static (internal to gistvacuum.c) and serves as the core page-processing routine

## Simplified Source

```c
static void
gistvacuumpage(GistVacState *vstate, BlockNumber blkno, BlockNumber orig_blkno)
{
    IndexVacuumInfo *info = vstate->info;
    IndexBulkDeleteCallback callback = vstate->callback;
    void *callback_state = vstate->callback_state;
    Relation rel = info->index;
    Buffer buffer;
    Page page;
    BlockNumber recurse_to;

restart:
    recurse_to = InvalidBlockNumber;

    // Allow vacuum delay and read the page
    vacuum_delay_point();
    buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, info->strategy);
    LockBuffer(buffer, GIST_EXCLUSIVE);
    page = (Page) BufferGetPage(buffer);

    if (gistPageRecyclable(page)) {
        // Page can be recycled immediately
        RecordFreeIndexPage(rel, blkno);
        vstate->stats->pages_deleted++;
        vstate->stats->pages_free++;
    }
    else if (GistPageIsDeleted(page)) {
        // Page already deleted but not recyclable yet
        vstate->stats->pages_deleted++;
    }
    else if (GistPageIsLeaf(page)) {
        // Process leaf page
        OffsetNumber todelete[MaxOffsetNumber];
        int ntodelete = 0;
        int nremain;
        GISTPageOpaque opaque = GistPageGetOpaque(page);
        OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

        // Check for concurrent page splits that require recursion
        if ((GistFollowRight(page) || vstate->startNSN < GistPageGetNSN(page)) &&
            (opaque->rightlink != InvalidBlockNumber) &&
            (opaque->rightlink < orig_blkno)) {
            recurse_to = opaque->rightlink;
        }

        // Find tuples to delete using callback
        if (callback) {
            for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off)) {
                ItemId iid = PageGetItemId(page, off);
                IndexTuple idxtuple = (IndexTuple) PageGetItem(page, iid);
                if (callback(&(idxtuple->t_tid), callback_state))
                    todelete[ntodelete++] = off;
            }
        }

        // Delete tuples if any were marked
        if (ntodelete > 0) {
            START_CRIT_SECTION();
            MarkBufferDirty(buffer);
            PageIndexMultiDelete(page, todelete, ntodelete);
            GistMarkTuplesDeleted(page);

            // Write WAL record
            if (RelationNeedsWAL(rel)) {
                XLogRecPtr recptr = gistXLogUpdate(buffer, todelete, ntodelete,
                                                  NULL, 0, InvalidBuffer);
                PageSetLSN(page, recptr);
            } else {
                PageSetLSN(page, gistGetFakeLSN(rel));
            }
            END_CRIT_SECTION();

            vstate->stats->tuples_removed += ntodelete;
            maxoff = PageGetMaxOffsetNumber(page);
        }

        // Check if page is now empty
        nremain = maxoff - FirstOffsetNumber + 1;
        if (nremain == 0) {
            // Mark empty page for later deletion
            if (blkno == orig_blkno)
                intset_add_member(vstate->empty_leaf_set, blkno);
        } else {
            vstate->stats->num_index_tuples += nremain;
        }
    }
    else {
        // Process internal page - check for invalid tuples
        OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
        for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off)) {
            ItemId iid = PageGetItemId(page, off);
            IndexTuple idxtuple = (IndexTuple) PageGetItem(page, iid);
            if (GistTupleIsInvalid(idxtuple))
                ereport(LOG, (errmsg("index \"%s\" contains an inner tuple marked as invalid",
                                   RelationGetRelationName(rel))));
        }

        // Track internal page for empty page deletion phase
        if (blkno == orig_blkno)
            intset_add_member(vstate->internal_page_set, blkno);
    }

    UnlockReleaseBuffer(buffer);

    // Handle tail recursion for page splits
    if (recurse_to != InvalidBlockNumber) {
        blkno = recurse_to;
        goto restart;
    }
}
```