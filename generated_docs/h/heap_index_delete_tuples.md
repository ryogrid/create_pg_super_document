# heap_index_delete_tuples

## Location
[src/backend/access/heap/heapam.c:8095-8403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L8095-L8403)

## Overview
Heapam implementation of tableam's index_delete_tuples interface that efficiently deletes multiple index tuples by examining their corresponding heap tuples and determining which are safe to delete.

## Definition
```c
TransactionId heap_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
```

## Detailed Description
This function is the core implementation of bulk index tuple deletion for heap tables. It serves as the heapam-specific implementation of the tableam interface, handling both simple index deletion and bottom-up index deletion operations.

The function performs several key operations:

1. **Sorting and Optimization**: Sorts the deltids array by TID and, for bottom-up deletion, reorders them to prioritize blocks with the most promising deletion candidates

2. **Prefetching**: Uses buffer prefetching to minimize I/O latency when accessing multiple heap pages, with prefetch distance determined by maintenance_io_concurrency settings

3. **Tuple Validation**: For each heap tuple referenced by index entries:
   - Validates the TID for corruption detection
   - Checks if the entire HOT chain is vacuumable using heap_hot_search_buffer
   - Determines if the tuple can be safely deleted

4. **Conflict Horizon Management**: Maintains a snapshotConflictHorizon by examining tuple headers throughout HOT chains to ensure recovery conflicts are properly handled

5. **Bottom-up Optimization**: For bottom-up deletion operations, implements space-based termination logic that stops processing when sufficient space has been freed

The function handles HOT (Heap-Only Tuples) chains by traversing from the index-referenced tuple through the entire chain, examining each tuple's visibility and updating the conflict horizon accordingly.

## Parameters / Member Variables
- `rel`: The heap relation containing the tuples to be deleted
- `delstate`: TM_IndexDeleteOp structure containing:
  - `deltids`: Array of TM_IndexDelete entries (TIDs to potentially delete)
  - `status`: Array of TM_IndexStatus entries tracking deletion status
  - `ndeltids`: Number of entries in deltids array
  - `bottomup`: Boolean indicating if this is a bottom-up deletion
  - `bottomupfreespace`: Target free space for bottom-up operations

## Dependencies
- Functions called/Symbols referenced:
  - [index_delete_sort](../i/index_delete_sort.md)
  - [bottomup_sort_and_shrink](../b/bottomup_sort_and_shrink.md)
  - [index_delete_prefetch_buffer](../i/index_delete_prefetch_buffer.md)
  - [index_delete_check_htid](../i/index_delete_check_htid.md)
  - [heap_hot_search_buffer](heap_hot_search_buffer.md)
  - [HeapTupleHeaderAdvanceConflictHorizon](../H/HeapTupleHeaderAdvanceConflictHorizon.md)
  - InitNonVacuumableSnapshot
  - [ReadBuffer](../R/ReadBuffer.md) / UnlockReleaseBuffer
  - [PageGetItemId](../P/PageGetItemId.md) / PageGetItem
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md) / ItemPointerGetOffsetNumber
  - HeapTupleHeaderGetXmin / HeapTupleHeaderGetUpdateXid
  - [IsCatalogRelation](../I/IsCatalogRelation.md)
  - [get_tablespace_maintenance_io_concurrency](../g/get_tablespace_maintenance_io_concurrency.md)
- Called from (representative examples):
  - Index AM implementations via tableam interface

## Notes and Other Information
- Supports two deletion modes: simple deletion and bottom-up deletion (space-driven optimization)
- Uses sophisticated prefetching strategy to minimize I/O costs when processing hundreds of tuples
- Implements comprehensive corruption detection through index_delete_check_htid
- Handles HOT chains by traversing from index-pointed tuple through the entire update chain  
- For bottom-up deletion, implements intelligent termination when space targets are met
- Returns InvalidTransactionId conflict horizon when no conflicts are needed
- Final deltids array may be shrunk to exclude non-deletable entries
- Critical for index AM performance during bulk deletion operations like VACUUM

## Simplified Source

```c
TransactionId heap_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
    TransactionId snapshotConflictHorizon = InvalidTransactionId;
    BlockNumber blkno = InvalidBlockNumber;
    Buffer buf = InvalidBuffer;
    Page page = NULL;
    OffsetNumber maxoff = InvalidOffsetNumber;
    SnapshotData SnapshotNonVacuumable;
    int finalndeltids = 0, nblocksaccessed = 0;

    // Bottom-up deletion state
    int nblocksfavorable = 0;
    int curtargetfreespace = delstate->bottomupfreespace;
    int actualfreespace = 0;
    bool bottomup_final_block = false;

    InitNonVacuumableSnapshot(SnapshotNonVacuumable, GlobalVisTestFor(rel));

    // Sort deltids array by TID
    index_delete_sort(delstate);

    // For bottom-up deletion: reorder by promising blocks and shrink array
    if (delstate->bottomup)
        nblocksfavorable = bottomup_sort_and_shrink(delstate);

    // Set up prefetching for better I/O performance
    setup_prefetching(rel, delstate, nblocksfavorable);

    // Process each TID to determine deletability
    for (int i = 0; i < delstate->ndeltids; i++)
    {
        TM_IndexDelete *ideltid = &delstate->deltids[i];
        TM_IndexStatus *istatus = delstate->status + ideltid->id;
        ItemPointer htid = &ideltid->tid;

        // Read new heap page if needed
        if (blkno == InvalidBlockNumber ||
            ItemPointerGetBlockNumber(htid) != blkno)
        {
            // Bottom-up early termination logic
            if (delstate->bottomup && should_terminate_bottomup(/* conditions */))
                break;

            // Switch to new page
            if (BufferIsValid(buf))
                UnlockReleaseBuffer(buf);

            blkno = ItemPointerGetBlockNumber(htid);
            buf = ReadBuffer(rel, blkno);
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);
            maxoff = PageGetMaxOffsetNumber(page);
            nblocksaccessed++;

            // Continue prefetching
            continue_prefetching();
        }

        // Validate TID for corruption detection
        index_delete_check_htid(delstate, page, maxoff, htid, istatus);

        // Check if entire HOT chain is deletable
        if (!istatus->knowndeletable)
        {
            ItemPointerData tmp = *htid;
            HeapTupleData heapTuple;

            // Test if any tuples in HOT chain are still visible
            if (heap_hot_search_buffer(&tmp, rel, buf, &SnapshotNonVacuumable,
                                      &heapTuple, NULL, true))
                continue;  // Can't delete - some tuples still visible

            // Mark as deletable and update free space tracking
            istatus->knowndeletable = true;
            if (delstate->bottomup)
            {
                actualfreespace += istatus->freespace;
                if (actualfreespace >= curtargetfreespace)
                    bottomup_final_block = true;
            }
        }

        // Update conflict horizon by traversing HOT chain
        OffsetNumber offnum = ItemPointerGetOffsetNumber(htid);
        TransactionId priorXmax = InvalidTransactionId;

        while (offnum >= FirstOffsetNumber && offnum <= maxoff)
        {
            ItemId lp = PageGetItemId(page, offnum);

            // Follow redirections
            if (ItemIdIsRedirected(lp))
            {
                offnum = ItemIdGetRedirect(lp);
                continue;
            }

            // Skip dead or invalid items
            if (!ItemIdIsNormal(lp))
                break;

            HeapTupleHeader htup = (HeapTupleHeader) PageGetItem(page, lp);

            // Validate HOT chain continuity
            if (TransactionIdIsValid(priorXmax) &&
                !TransactionIdEquals(HeapTupleHeaderGetXmin(htup), priorXmax))
                break;

            // Update conflict horizon
            HeapTupleHeaderAdvanceConflictHorizon(htup, &snapshotConflictHorizon);

            // Stop if not HOT-updated (end of chain)
            if (!HeapTupleHeaderIsHotUpdated(htup))
                break;

            // Move to next tuple in HOT chain
            offnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
            priorXmax = HeapTupleHeaderGetUpdateXid(htup);
        }

        finalndeltids = i + 1;
    }

    UnlockReleaseBuffer(buf);

    // Shrink array to exclude non-deletable entries
    delstate->ndeltids = finalndeltids;
    return snapshotConflictHorizon;
}
```