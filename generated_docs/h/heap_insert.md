# heap_insert

## Location
[src/backend/access/heap/heapam.c:2038-2228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L2038-L2228)

## Overview
heap_insert inserts a tuple into a heap relation with full transactional support, WAL logging, and proper buffer management.

## Definition
```c
void heap_insert(Relation relation, HeapTuple tup, CommandId cid, int options, BulkInsertState bistate);
```

## Detailed Description
heap_insert is the core function for inserting tuples into heap tables in PostgreSQL. It handles the complete insertion process including tuple preparation, buffer allocation, visibility map management, WAL logging, and cache invalidation. The function stamps the tuple with the current transaction ID and specified command ID, manages toasting of large values, and ensures proper concurrency control through serializable conflict detection.

The insertion process involves several critical steps: preparing the tuple (including toasting if necessary), finding an appropriate buffer for insertion, checking for serializable conflicts, performing the actual insertion within a critical section, managing visibility maps, creating WAL records for crash recovery, and updating statistics. The function also handles speculative insertions used for unique constraint enforcement.

## Parameters / Member Variables
- `relation`: The heap relation where the tuple will be inserted
- `tup`: The HeapTuple to be inserted (original untoasted data)
- `cid`: Command ID to stamp on the tuple
- `options`: Insertion option flags (HEAP_INSERT_* constants)
- `bistate`: BulkInsertState for optimized bulk operations (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [heap_prepare_insert](heap_prepare_insert.md)
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md)
  - [RelationGetBufferForTuple](../R/RelationGetBufferForTuple.md)
  - [CheckForSerializableConflictIn](../C/CheckForSerializableConflictIn.md)
  - [RelationPutHeapTuple](../R/RelationPutHeapTuple.md)
  - [visibilitymap_clear](../v/visibilitymap_clear.md)
  - [XLogInsert](../X/XLogInsert.md) (and related WAL functions)
  - [CacheInvalidateHeapTuple](../C/CacheInvalidateHeapTuple.md)
  - [pgstat_count_heap_insert](../p/pgstat_count_heap_insert.md)
  - [heap_freetuple](heap_freetuple.md)
- Called from (representative examples):
  - [simple_heap_insert](../s/simple_heap_insert.md)
  - [heapam_tuple_insert](heapam_tuple_insert.md)
  - [heapam_tuple_insert_speculative](heapam_tuple_insert_speculative.md)
  - [toast_save_datum](../t/toast_save_datum.md)

## Notes and Other Information
- Updates tup->t_self with the actual TID where the tuple was stored
- Handles both regular and speculative insertions (for unique constraint checking)
- Manages visibility map updates when inserting into all-visible pages
- Includes comprehensive WAL logging for crash recovery and replication
- Supports bulk insert optimization through BulkInsertState parameter
- Toasted field values are not reflected back into the original tuple structure
- Critical sections ensure atomicity of buffer modifications and WAL logging
- Includes logic for logical decoding support in replication scenarios

## Simplified Source

```c
void heap_insert(Relation relation, HeapTuple tup, CommandId cid,
                int options, BulkInsertState bistate) {
    TransactionId xid = GetCurrentTransactionId();
    HeapTuple heaptup;
    Buffer buffer;
    Buffer vmbuffer = InvalidBuffer;
    bool all_visible_cleared = false;

    // Validate tuple matches relation structure
    Assert(HeapTupleHeaderGetNatts(tup->t_data) <=
           RelationGetNumberOfAttributes(relation));

    // Prepare tuple: fill headers, handle toasting
    heaptup = heap_prepare_insert(relation, tup, xid, cid, options);

    // Find buffer with space for this tuple
    buffer = RelationGetBufferForTuple(relation, heaptup->t_len,
                                      InvalidBuffer, options, bistate,
                                      &vmbuffer, NULL, 0);

    // Check for serializable conflicts before inserting
    CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

    // Critical section: no errors allowed until changes are logged
    START_CRIT_SECTION();

    // Insert tuple into buffer
    RelationPutHeapTuple(relation, buffer, heaptup,
                        (options & HEAP_INSERT_SPECULATIVE) != 0);

    // Clear visibility map if page was all-visible
    if (PageIsAllVisible(BufferGetPage(buffer))) {
        all_visible_cleared = true;
        PageClearAllVisible(BufferGetPage(buffer));
        visibilitymap_clear(relation,
                           ItemPointerGetBlockNumber(&(heaptup->t_self)),
                           vmbuffer, VISIBILITYMAP_VALID_BITS);
    }

    // Mark buffer as modified
    MarkBufferDirty(buffer);

    // Create WAL record for crash recovery
    if (RelationNeedsWAL(relation)) {
        xl_heap_insert xlrec;
        xl_heap_header xlhdr;
        XLogRecPtr recptr;
        uint8 info = XLOG_HEAP_INSERT;
        int bufflags = 0;

        // Setup WAL record data
        xlrec.offnum = ItemPointerGetOffsetNumber(&heaptup->t_self);
        xlrec.flags = 0;
        if (all_visible_cleared)
            xlrec.flags |= XLH_INSERT_ALL_VISIBLE_CLEARED;
        if (options & HEAP_INSERT_SPECULATIVE)
            xlrec.flags |= XLH_INSERT_IS_SPECULATIVE;

        // Register WAL data and write record
        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfHeapInsert);

        xlhdr.t_infomask2 = heaptup->t_data->t_infomask2;
        xlhdr.t_infomask = heaptup->t_data->t_infomask;
        xlhdr.t_hoff = heaptup->t_data->t_hoff;

        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD | bufflags);
        XLogRegisterBufData(0, (char *) &xlhdr, SizeOfHeapHeader);
        XLogRegisterBufData(0, (char *) heaptup->t_data + SizeofHeapTupleHeader,
                           heaptup->t_len - SizeofHeapTupleHeader);

        recptr = XLogInsert(RM_HEAP_ID, info);
        PageSetLSN(BufferGetPage(buffer), recptr);
    }

    END_CRIT_SECTION();

    // Release buffers
    UnlockReleaseBuffer(buffer);
    if (vmbuffer != InvalidBuffer)
        ReleaseBuffer(vmbuffer);

    // Cache invalidation and statistics
    CacheInvalidateHeapTuple(relation, heaptup, NULL);
    pgstat_count_heap_insert(relation, 1);

    // Copy TID back to caller's tuple and cleanup
    if (heaptup != tup) {
        tup->t_self = heaptup->t_self;
        heap_freetuple(heaptup);
    }
}
```