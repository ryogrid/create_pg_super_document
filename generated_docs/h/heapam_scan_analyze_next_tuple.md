# heapam_scan_analyze_next_tuple

## Location
[src/backend/access/heap/heapam_handler.c:1030-1172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L1030-L1172)

## Overview
This function retrieves and evaluates the next tuple during ANALYZE operations, determining its visibility status and maintaining statistical counters for live and dead tuples.

## Definition
static bool heapam_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin, double *liverows, double *deadrows, TupleTableSlot *slot)

## Detailed Description
heapam_scan_analyze_next_tuple is a core function for ANALYZE operations that iterates through tuples on the current heap page and determines their sampling eligibility based on transaction visibility. The function examines each tuple's visibility state using HeapTupleSatisfiesVacuum and updates counters for live and dead rows accordingly. It handles various tuple states including insert-in-progress and delete-in-progress transactions with special logic for transactions belonging to the current session. When a suitable tuple is found for sampling, it stores the tuple in the provided slot and returns true. When all tuples on the current page have been processed, it releases the buffer lock and returns false.

## Parameters / Member Variables
- `scan`: TableScanDesc representing the heap scan descriptor for the ANALYZE operation
- `OldestXmin`: Transaction ID threshold for determining tuple visibility
- `liverows`: Pointer to counter tracking the number of live tuples encountered
- `deadrows`: Pointer to counter tracking the number of dead tuples encountered  
- `slot`: TupleTableSlot to store the selected tuple for sampling

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsNormal, ItemIdIsDead
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - RelationGetRelid
  - [PageGetItem](../P/PageGetItem.md)
  - ItemIdGetLength
  - [HeapTupleSatisfiesVacuum](../H/HeapTupleSatisfiesVacuum.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - HeapTupleHeaderGetXmin, HeapTupleHeaderGetUpdateXid
  - [ExecStoreBufferHeapTuple](../E/ExecStoreBufferHeapTuple.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
- Constants referenced:
  - HEAPTUPLE_LIVE, HEAPTUPLE_DEAD, HEAPTUPLE_RECENTLY_DEAD
  - HEAPTUPLE_INSERT_IN_PROGRESS, HEAPTUPLE_DELETE_IN_PROGRESS
  - InvalidBuffer
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (referenced in heapam_handler.c:2634)

## Notes and Other Information
- This is a static function, only accessible within heapam_handler.c
- Returns true when a tuple is selected for sampling, false when the page is exhausted
- Handles complex transaction visibility logic for in-progress transactions
- [Insert](../I/Insert.md)-in-progress tuples are only counted/sampled if they belong to the current transaction
- [Delete](../D/Delete.md)-in-progress tuples are treated as live unless deleted by the current transaction
- Ignores unused and redirect line pointers but counts DEAD line pointers as dead rows
- Maintains buffer lock throughout tuple processing until a sample tuple is found
- Releases buffer lock and clears slot when no more tuples are available on the page
- Works in conjunction with heapam_scan_analyze_next_block() for complete page analysis
- The slot parameter must be a BufferHeapTupleTableSlot (checked with TTS_IS_BUFFERTUPLE)

## Simplified Source

```c
static bool heapam_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
                                          double *liverows, double *deadrows,
                                          TupleTableSlot *slot) {
    HeapScanDesc hscan = (HeapScanDesc) scan;
    Page targpage = BufferGetPage(hscan->rs_cbuf);
    OffsetNumber maxoffset = PageGetMaxOffsetNumber(targpage);
    BufferHeapTupleTableSlot *hslot = (BufferHeapTupleTableSlot *) slot;

    // Scan through all tuples on the current page
    for (; hscan->rs_cindex <= maxoffset; hscan->rs_cindex++) {
        ItemId itemid = PageGetItemId(targpage, hscan->rs_cindex);
        HeapTuple targtuple = &hslot->base.tupdata;
        bool sample_it = false;

        // Skip unused/redirect items, count dead items
        if (!ItemIdIsNormal(itemid)) {
            if (ItemIdIsDead(itemid))
                *deadrows += 1;
            continue;
        }

        // Set up tuple data
        ItemPointerSet(&targtuple->t_self, hscan->rs_cblock, hscan->rs_cindex);
        targtuple->t_tableOid = RelationGetRelid(scan->rs_rd);
        targtuple->t_data = (HeapTupleHeader) PageGetItem(targpage, itemid);
        targtuple->t_len = ItemIdGetLength(itemid);

        // Check tuple visibility and decide whether to sample
        switch (HeapTupleSatisfiesVacuum(targtuple, OldestXmin, hscan->rs_cbuf)) {
            case HEAPTUPLE_LIVE:
                sample_it = true;
                *liverows += 1;
                break;

            case HEAPTUPLE_DEAD:
            case HEAPTUPLE_RECENTLY_DEAD:
                *deadrows += 1;
                break;

            case HEAPTUPLE_INSERT_IN_PROGRESS:
                // Only sample if it's our own transaction
                if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(targtuple->t_data))) {
                    sample_it = true;
                    *liverows += 1;
                }
                break;

            case HEAPTUPLE_DELETE_IN_PROGRESS:
                // Count as dead if our transaction deleted it, otherwise live
                if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(targtuple->t_data)))
                    *deadrows += 1;
                else {
                    sample_it = true;
                    *liverows += 1;
                }
                break;
        }

        // If we found a tuple to sample, return it
        if (sample_it) {
            ExecStoreBufferHeapTuple(targtuple, slot, hscan->rs_cbuf);
            hscan->rs_cindex++;
            return true;  // Buffer remains locked
        }
    }

    // No more tuples on page - release buffer and clear slot
    UnlockReleaseBuffer(hscan->rs_cbuf);
    hscan->rs_cbuf = InvalidBuffer;
    ExecClearTuple(slot);
    return false;
}
```