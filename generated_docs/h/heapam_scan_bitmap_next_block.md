# heapam_scan_bitmap_next_block

## Location
[src/backend/access/heap/heapam_handler.c:2122-2254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L2122-L2254)

## Overview
Processes the next block during a bitmap heap scan, examining tuples on the specified block and collecting visible tuples for subsequent retrieval.

## Definition

```c
static bool
heapam_scan_bitmap_next_block(TableScanDesc scan,
							  TBMIterateResult *tbmres)
```
## Detailed Description
This function is a core component of bitmap heap scans, responsible for processing individual blocks identified by a bitmap index scan. It determines which tuples on a given block are visible to the current transaction and stores their offsets for later tuple retrieval. The function handles both lossy and non-lossy bitmap results, employs optimizations for all-visible pages, and manages HOT (Heap-Only Tuples) chains appropriately. It also performs necessary locking, pruning, and visibility checks while maintaining transaction isolation guarantees.

## Parameters / Member Variables
- : The table scan descriptor containing scan state and parameters
- : Bitmap iterator result containing block number, tuple offsets, and metadata about the block

## Dependencies
- Functions called/Symbols referenced:
  - VM_ALL_VISIBLE (visibility map check)
  - IsolationIsSerializable (isolation level check)
  - [ReleaseAndReadBuffer](../R/ReleaseAndReadBuffer.md) (buffer management)
  - [heap_page_prune_opt](heap_page_prune_opt.md) (page maintenance)
  - [heap_hot_search_buffer](heap_hot_search_buffer.md) (HOT chain traversal)
  - [HeapTupleSatisfiesVisibility](../H/HeapTupleSatisfiesVisibility.md) (tuple visibility)
  - [PredicateLockTID](../P/PredicateLockTID.md) (predicate locking)
  - [HeapCheckForSerializableConflictOut](../H/HeapCheckForSerializableConflictOut.md) (serializable isolation)
  - Various page and item manipulation functions
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (as part of table access method interface)

## Notes and Other Information
- Implements smart optimizations: skips fetching pages when tuples aren't needed and all tuples are visible
- Handles both lossy and non-lossy bitmap cases with different strategies
- For non-lossy bitmaps: follows HOT chains from specific offsets
- For lossy bitmaps: examines every line pointer on the page
- Maintains proper buffer locking discipline for concurrent safety
- Respects transaction isolation levels, especially SERIALIZABLE
- Updates scan state (rs_ntuples, rs_vistuples) for subsequent tuple fetching
- Returns true if any visible tuples were found on the block

## Simplified Source

```c
static bool heapam_scan_bitmap_next_block(TableScanDesc scan, TBMIterateResult *tbmres) {
    HeapScanDesc hscan = (HeapScanDesc) scan;
    BlockNumber block = tbmres->blockno;
    Buffer buffer;
    Snapshot snapshot;
    int ntup = 0;

    hscan->rs_cindex = 0;
    hscan->rs_ntuples = 0;

    // Optimization: skip page if we don't need tuples, no recheck needed, and all visible
    if (!(scan->rs_flags & SO_NEED_TUPLES) && !tbmres->recheck &&
        VM_ALL_VISIBLE(scan->rs_rd, tbmres->blockno, &hscan->rs_vmbuffer)) {
        hscan->rs_empty_tuples_pending += tbmres->ntuples;
        return true;
    }

    // Skip blocks beyond relation end (except in SERIALIZABLE isolation)
    if (!IsolationIsSerializable() && block >= hscan->rs_nblocks)
        return false;

    // Get buffer for the target block
    hscan->rs_cbuf = ReleaseAndReadBuffer(hscan->rs_cbuf, scan->rs_rd, block);
    hscan->rs_cblock = block;
    buffer = hscan->rs_cbuf;
    snapshot = scan->rs_snapshot;

    // Prune page to clean up dead tuples
    heap_page_prune_opt(scan->rs_rd, buffer);

    // Lock buffer for visibility checks
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    if (tbmres->ntuples >= 0) {
        // Non-lossy bitmap: check specific offsets and follow HOT chains
        for (int curslot = 0; curslot < tbmres->ntuples; curslot++) {
            OffsetNumber offnum = tbmres->offsets[curslot];
            ItemPointerData tid;
            HeapTupleData heapTuple;

            ItemPointerSet(&tid, block, offnum);
            if (heap_hot_search_buffer(&tid, scan->rs_rd, buffer, snapshot,
                                      &heapTuple, NULL, true))
                hscan->rs_vistuples[ntup++] = ItemPointerGetOffsetNumber(&tid);
        }
    } else {
        // Lossy bitmap: examine every line pointer on the page
        Page page = BufferGetPage(buffer);
        OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

        for (OffsetNumber offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
            ItemId lp = PageGetItemId(page, offnum);
            if (!ItemIdIsNormal(lp))
                continue;

            HeapTupleData loctup;
            loctup.t_data = (HeapTupleHeader) PageGetItem(page, lp);
            loctup.t_len = ItemIdGetLength(lp);
            loctup.t_tableOid = scan->rs_rd->rd_id;
            ItemPointerSet(&loctup.t_self, block, offnum);

            bool valid = HeapTupleSatisfiesVisibility(&loctup, snapshot, buffer);
            if (valid) {
                hscan->rs_vistuples[ntup++] = offnum;
                PredicateLockTID(scan->rs_rd, &loctup.t_self, snapshot,
                               HeapTupleHeaderGetXmin(loctup.t_data));
            }
            HeapCheckForSerializableConflictOut(valid, scan->rs_rd, &loctup, buffer, snapshot);
        }
    }

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    hscan->rs_ntuples = ntup;
    return ntup > 0;
}
```