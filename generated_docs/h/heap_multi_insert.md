# heap_multi_insert

## Location
[src/backend/access/heap/heapam.c:2309-2672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L2309-L2672)

## Overview
heap_multi_insert efficiently inserts multiple tuples into a heap relation in one operation, optimizing performance by batching WAL records and minimizing page lock operations when multiple tuples fit on the same page.

## Definition

```c
struct from the scratch area */
			xlrec = (xl_heap_multi_insert *) scratchptr;
```
## Detailed Description
This function is an optimized version of heap_insert() for inserting multiple tuples simultaneously. It processes tuples by first preparing them all through heap_prepare_insert(), then inserting them page by page to minimize I/O operations. When multiple tuples fit on a single page, it writes only one WAL record covering all tuples and locks/unlocks the page once. The function handles serializable conflict detection, visibility map updates, logical decoding requirements, and proper transaction logging. It also manages relation extension by calculating required pages in advance using heap_multi_insert_pages().

## Parameters / Member Variables
- : The target heap relation for tuple insertion
- : Array of TupleTableSlot pointers containing the tuples to insert
- : Number of tuples to insert from the slots array
- : Command ID for the current command within the transaction
- : Insertion options flags (e.g., HEAP_INSERT_FROZEN)
- : Bulk insert state for optimizing buffer management

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md)
  - [heap_prepare_insert](heap_prepare_insert.md)
  - [heap_multi_insert_pages](heap_multi_insert_pages.md)
  - [RelationGetBufferForTuple](../R/RelationGetBufferForTuple.md)
  - [RelationPutHeapTuple](../R/RelationPutHeapTuple.md)
  - [CheckForSerializableConflictIn](../C/CheckForSerializableConflictIn.md)
  - [ExecFetchSlotHeapTuple](../E/ExecFetchSlotHeapTuple.md)
  - [visibilitymap_clear](../v/visibilitymap_clear.md)
  - [visibilitymap_set](../v/visibilitymap_set.md)
  - [XLogInsert](../X/XLogInsert.md) (and related WAL functions)
- Called from:
  - [CatalogTuplesMultiInsertWithInfo](../C/CatalogTuplesMultiInsertWithInfo.md)
  - Various bulk insert operations

## Notes and Other Information
- This function leaks memory into the current memory context; create a temporary context if needed
- Currently does not support HEAP_INSERT_NO_LOGICAL option
- Performs serializable conflict checks both before and after insertion for correctness
- Handles visibility map updates for frozen and all-visible pages
- Supports logical decoding by including necessary tuple data and CID logging
- Uses critical sections to ensure atomicity of page modifications
- Optimizes relation extension by pre-calculating required pages
- Updates statistics via pgstat_count_heap_insert() upon completion

## Simplified Source

```c
void
heap_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
                  CommandId cid, int options, BulkInsertState bistate)
{
    TransactionId xid = GetCurrentTransactionId();
    HeapTuple *heaptuples;
    Buffer vmbuffer = InvalidBuffer;
    bool needwal = RelationNeedsWAL(relation);
    bool need_cids = RelationIsAccessibleInLogicalDecoding(relation);
    Size saveFreeSpace = RelationGetTargetPageFreeSpace(relation, HEAP_DEFAULT_FILLFACTOR);

    Assert(!(options & HEAP_INSERT_NO_LOGICAL));
    AssertHasSnapshotForToast(relation);

    // Phase 1: Prepare all tuples for insertion
    heaptuples = palloc(ntuples * sizeof(HeapTuple));
    for (int i = 0; i < ntuples; i++)
    {
        HeapTuple tuple = ExecFetchSlotHeapTuple(slots[i], true, NULL);
        slots[i]->tts_tableOid = RelationGetRelid(relation);
        tuple->t_tableOid = slots[i]->tts_tableOid;
        heaptuples[i] = heap_prepare_insert(relation, tuple, xid, cid, options);
    }

    // Check for serializable conflicts before starting
    CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

    // Phase 2: Insert tuples page by page
    int ndone = 0;
    while (ndone < ntuples)
    {
        Buffer buffer;
        bool all_visible_cleared = false;
        bool all_frozen_set = false;

        CHECK_FOR_INTERRUPTS();

        // Get buffer for next batch of tuples
        buffer = RelationGetBufferForTuple(relation, heaptuples[ndone]->t_len,
                                         InvalidBuffer, options, bistate,
                                         &vmbuffer, NULL, /* npages calculation */);
        Page page = BufferGetPage(buffer);

        bool starting_with_empty_page = (PageGetMaxOffsetNumber(page) == 0);
        if (starting_with_empty_page && (options & HEAP_INSERT_FROZEN))
            all_frozen_set = true;

        START_CRIT_SECTION();

        // Insert first tuple and log CID if needed
        RelationPutHeapTuple(relation, buffer, heaptuples[ndone], false);
        if (needwal && need_cids)
            log_heap_new_cid(relation, heaptuples[ndone]);

        // Add as many additional tuples as fit on this page
        int nthispage = 1;
        for (; ndone + nthispage < ntuples; nthispage++)
        {
            HeapTuple heaptup = heaptuples[ndone + nthispage];

            if (PageGetHeapFreeSpace(page) < MAXALIGN(heaptup->t_len) + saveFreeSpace)
                break;

            RelationPutHeapTuple(relation, buffer, heaptup, false);
            if (needwal && need_cids)
                log_heap_new_cid(relation, heaptup);
        }

        // Handle visibility map updates
        if (PageIsAllVisible(page) && !(options & HEAP_INSERT_FROZEN))
        {
            all_visible_cleared = true;
            PageClearAllVisible(page);
            visibilitymap_clear(relation, BufferGetBlockNumber(buffer),
                              vmbuffer, VISIBILITYMAP_VALID_BITS);
        }
        else if (all_frozen_set)
            PageSetAllVisible(page);

        MarkBufferDirty(buffer);

        // Write WAL record for all tuples on this page
        if (needwal)
        {
            // Construct and write multi-insert WAL record
            // [Complex WAL record construction omitted for brevity]
            XLogRecPtr recptr = XLogInsert(RM_HEAP2_ID, /* info flags */);
            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();

        // Update visibility map for frozen pages
        if (all_frozen_set)
        {
            visibilitymap_set(relation, BufferGetBlockNumber(buffer), buffer,
                            InvalidXLogRecPtr, vmbuffer, InvalidTransactionId,
                            VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN);
        }

        UnlockReleaseBuffer(buffer);
        ndone += nthispage;
    }

    if (vmbuffer != InvalidBuffer)
        ReleaseBuffer(vmbuffer);

    // Final serializable conflict check
    CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

    // Cache invalidation for catalog relations
    if (IsCatalogRelation(relation))
    {
        for (int i = 0; i < ntuples; i++)
            CacheInvalidateHeapTuple(relation, heaptuples[i], NULL);
    }

    // Copy t_self fields back to caller's slots and update statistics
    for (int i = 0; i < ntuples; i++)
        slots[i]->tts_tid = heaptuples[i]->t_self;

    pgstat_count_heap_insert(relation, ntuples);
}
```