# _bt_dedup_pass

## Location
[src/backend/access/nbtree/nbtdedup.c:58-306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtdedup.c#L58-L306)

## Overview
Performs a B-tree deduplication pass to merge duplicate index tuples into posting list tuples, freeing up page space to potentially avoid page splits.

## Definition

```c
void
_bt_dedup_pass(Relation rel, Buffer buf, IndexTuple newitem, Size newitemsz,
			   bool bottomupdedup)
```
## Detailed Description
This function implements the core B-tree deduplication algorithm that scans through index tuples on a page and merges duplicates into posting list tuples to save space. The function uses two different strategies:

1. **General deduplication**: Merges as many duplicates as possible to maximize space savings
2. **Single value strategy**: For pages full of tuples with a single value, leaves some tuples untouched at the end to prepare for anticipated page splits

When called after a failed , the goal is to prevent page splits entirely by buying more time. The function will only proceed if it can free at least  bytes (plus line pointer overhead).

The deduplication process creates a new temporary page, copies tuples while merging duplicates, and then replaces the original page content. All changes are logged for WAL replay.

## Parameters / Member Variables
- `rel`: The index relation being processed
- `buf`: Buffer containing the page to be deduplicated
- `newitem`: New index tuple that needs to be inserted (used for space calculations)
- `newitemsz`: Size of the new item in bytes (MAXALIGNED, excluding line pointer)
- `bottomupdedup`: If true, indicates this call follows a failed bottom-up deletion pass
## Dependencies
- Functions called/Symbols referenced:
  - : Determines if single value strategy should be applied
  - : Initializes a new pending posting list
  - : Attempts to add tuple's heap TIDs to pending list
  - : Finalizes pending posting list and adds to page
  - : Adjusts posting list size for single value strategy
  - : Creates temporary page copy
  - : Replaces original page with modified version

- Called from (representative examples):
  - : Main deduplication entry point during insertion

## Notes and Other Information
- The function implements a "single value" strategy for pages containing many tuples of the same value, leaving some tuples unmerged to optimize for future page splits
- Space calculations include both tuple data and line pointer overhead
- The function clears the BTP_HAS_GARBAGE flag since heapkeyspace indexes don't use it
- WAL logging ensures crash recovery can replay the deduplication operation
- If no deduplication intervals are created, the function returns early without modifying the page
- The maxpostingsize is limited to 1/6 of a page to ensure good split points for pages with many duplicates

## Simplified Source

```c
void _bt_dedup_pass(Relation rel, Buffer buf, IndexTuple newitem, Size newitemsz, bool bottomupdedup) {
    Page page = BufferGetPage(buf);
    BTPageOpaque opaque = BTPageGetOpaque(page);
    Page newpage;
    BTDedupState state;
    bool singlevalstrat = false;
    int nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);

    // Include line pointer in size calculation
    newitemsz += sizeof(ItemIdData);

    // Initialize deduplication state
    state = (BTDedupState) palloc(sizeof(BTDedupStateData));
    state->deduplicate = true;
    state->maxpostingsize = Min(BTMaxItemSize(page) / 2, INDEX_SIZE_MASK);
    state->htids = palloc(state->maxpostingsize);
    // ... initialize other state fields

    OffsetNumber minoff = P_FIRSTDATAKEY(opaque);
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

    // Consider single value strategy for non-bottom-up calls
    if (!bottomupdedup)
        singlevalstrat = _bt_do_singleval(rel, page, state, minoff, newitem);

    // Create temporary page copy for deduplication
    newpage = PageGetTempPageCopySpecial(page);
    PageSetLSN(newpage, PageGetLSN(page));

    // Copy high key if present
    if (!P_RIGHTMOST(opaque)) {
        // Copy high key to new page
    }

    // Process each tuple on the page
    for (OffsetNumber offnum = minoff; offnum <= maxoff; offnum++) {
        ItemId itemid = PageGetItemId(page, offnum);
        IndexTuple itup = (IndexTuple) PageGetItem(page, itemid);

        if (offnum == minoff) {
            // Start first pending posting list
            _bt_dedup_start_pending(state, itup, offnum);
        }
        else if (state->deduplicate &&
                 _bt_keep_natts_fast(rel, state->base, itup) > nkeyatts &&
                 _bt_dedup_save_htid(state, itup)) {
            // Tuple matches current posting list - TIDs saved
        }
        else {
            // Finish current posting list and start new one
            _bt_dedup_finish_pending(newpage, state);

            // Apply single value strategy adjustments
            if (singlevalstrat) {
                if (state->nmaxitems == 5)
                    _bt_singleval_fillfactor(page, state, newitemsz);
                else if (state->nmaxitems == 6) {
                    state->deduplicate = false;
                    singlevalstrat = false;
                }
            }

            // Start new pending posting list
            _bt_dedup_start_pending(state, itup, offnum);
        }
    }

    // Finish the last pending posting list
    _bt_dedup_finish_pending(newpage, state);

    // Return early if no deduplication was possible
    if (state->nintervals == 0) {
        pfree(newpage);
        pfree(state->htids);
        pfree(state);
        return;
    }

    // Clear garbage flag and apply changes
    if (P_HAS_GARBAGE(opaque)) {
        BTPageOpaque nopaque = BTPageGetOpaque(newpage);
        nopaque->btpo_flags &= ~BTP_HAS_GARBAGE;
    }

    START_CRIT_SECTION();

    // Replace original page with deduplicated version
    PageRestoreTempPage(newpage, page);
    MarkBufferDirty(buf);

    // WAL logging for crash recovery
    if (RelationNeedsWAL(rel)) {
        XLogRecPtr recptr;
        xl_btree_dedup xlrec_dedup;

        xlrec_dedup.nintervals = state->nintervals;

        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
        XLogRegisterData((char *) &xlrec_dedup, SizeOfBtreeDedup);
        XLogRegisterBufData(0, (char *) state->intervals,
                           state->nintervals * sizeof(BTDedupInterval));

        recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_DEDUP);
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    // Cleanup allocated memory
    pfree(state->htids);
    pfree(state);
}
```