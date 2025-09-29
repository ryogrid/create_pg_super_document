# log_heap_prune_and_freeze

## Location
[src/backend/access/heap/pruneheap.c:2053-2172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/pruneheap.c#L2053-L2172)

## Overview
Writes an XLOG_HEAP2_PRUNE_FREEZE WAL record for various page maintenance operations including pruning, freezing, and vacuum cleanup operations.

## Definition
```c
void log_heap_prune_and_freeze(Relation relation, Buffer buffer,
                               TransactionId conflict_xid,
                               bool cleanup_lock,
                               PruneReason reason,
                               HeapTupleFreeze *frozen, int nfrozen,
                               OffsetNumber *redirected, int nredirected,
                               OffsetNumber *dead, int ndead,
                               OffsetNumber *unused, int nunused)
```

## Detailed Description
This function creates a unified WAL record that can handle multiple types of heap page maintenance operations. It consolidates page pruning (redirecting and marking items dead), freezing operations, and vacuum cleanup into a single record type (XLOG_HEAP2_PRUNE_FREEZE) to reduce WAL overhead.

The function registers buffer data for each type of operation present, using specialized data structures for efficient storage. For freeze operations, it calls `heap_log_freeze_plan` to deduplicate freeze plans before logging. The function sets appropriate flags in the WAL record to indicate which operations are included and handles special cases like catalog relations and conflict horizons for hot standby.

The function operates within a critical section and must be careful about resource usage and error handling.

## Parameters / Member Variables
- `relation`: The relation being operated on
- `buffer`: Buffer containing the heap page being modified
- `conflict_xid`: Transaction ID that might conflict with hot standby queries (for recovery)
- `cleanup_lock`: Whether replay requires a cleanup lock on the buffer
- `reason`: The reason for the prune operation (access, vacuum scan, or vacuum cleanup)
- `frozen`: Array of HeapTupleFreeze structures for freeze operations
- `nfrozen`: Number of tuples to be frozen
- `redirected`: Array of offset numbers for redirect operations
- `nredirected`: Number of redirect operations
- `dead`: Array of offset numbers for items to mark as dead
- `ndead`: Number of items to mark as dead
- `unused`: Array of offset numbers for items to mark as unused
- `nunused`: Number of items to mark as unused

## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [heap_log_freeze_plan](../h/heap_log_freeze_plan.md)
  - [XLogRegisterBufData](../X/XLogRegisterBufData.md)
  - RelationIsAccessibleInLogicalDecoding
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [BufferGetPage](../B/BufferGetPage.md)
- Called from (representative examples):
  - [heap_page_prune_and_freeze](../h/heap_page_prune_and_freeze.md)
  - [lazy_vacuum_heap_page](lazy_vacuum_heap_page.md)

## Notes and Other Information
This function is called within a critical section, so it must be efficient and avoid operations that could fail. The function destructively sorts the frozen tuples array through heap_log_freeze_plan. The unified record format allows for efficient WAL logging when multiple operations occur on the same page, which is common during vacuum operations. Different prune reasons result in different WAL record subtypes but use the same underlying record structure.

## Simplified Source

```c
void log_heap_prune_and_freeze(Relation relation, Buffer buffer,
                               TransactionId conflict_xid, bool cleanup_lock,
                               PruneReason reason, HeapTupleFreeze *frozen, int nfrozen,
                               OffsetNumber *redirected, int nredirected,
                               OffsetNumber *dead, int ndead,
                               OffsetNumber *unused, int nunused)
{
    xl_heap_prune xlrec;
    XLogRecPtr recptr;
    uint8 info;

    xlrec.flags = 0;

    // Begin WAL record construction
    XLogBeginInsert();
    XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

    // Register freeze operations if present
    if (nfrozen > 0) {
        xlrec.flags |= XLHP_HAS_FREEZE_PLANS;
        // Register deduplicated freeze plans
        int nplans = heap_log_freeze_plan(frozen, nfrozen, plans, frz_offsets);
        XLogRegisterBufData(0, (char *) &freeze_plans, sizeof(freeze_plans));
        XLogRegisterBufData(0, (char *) plans, sizeof(xlhp_freeze_plan) * nplans);
    }

    // Register redirect operations if present
    if (nredirected > 0) {
        xlrec.flags |= XLHP_HAS_REDIRECTIONS;
        XLogRegisterBufData(0, (char *) redirected, sizeof(OffsetNumber[2]) * nredirected);
    }

    // Register dead items if present
    if (ndead > 0) {
        xlrec.flags |= XLHP_HAS_DEAD_ITEMS;
        XLogRegisterBufData(0, (char *) dead, sizeof(OffsetNumber) * ndead);
    }

    // Register unused items if present
    if (nunused > 0) {
        xlrec.flags |= XLHP_HAS_NOW_UNUSED_ITEMS;
        XLogRegisterBufData(0, (char *) unused, sizeof(OffsetNumber) * nunused);
    }

    // Set special flags for catalog relations and conflicts
    if (RelationIsAccessibleInLogicalDecoding(relation))
        xlrec.flags |= XLHP_IS_CATALOG_REL;
    if (TransactionIdIsValid(conflict_xid))
        xlrec.flags |= XLHP_HAS_CONFLICT_HORIZON;
    if (cleanup_lock)
        xlrec.flags |= XLHP_CLEANUP_LOCK;

    // Register main record data
    XLogRegisterData((char *) &xlrec, SizeOfHeapPrune);
    if (TransactionIdIsValid(conflict_xid))
        XLogRegisterData((char *) &conflict_xid, sizeof(TransactionId));

    // Determine WAL record subtype based on prune reason
    switch (reason) {
        case PRUNE_ON_ACCESS:
            info = XLOG_HEAP2_PRUNE_ON_ACCESS;
            break;
        case PRUNE_VACUUM_SCAN:
            info = XLOG_HEAP2_PRUNE_VACUUM_SCAN;
            break;
        case PRUNE_VACUUM_CLEANUP:
            info = XLOG_HEAP2_PRUNE_VACUUM_CLEANUP;
            break;
    }

    // Write the WAL record and update page LSN
    recptr = XLogInsert(RM_HEAP2_ID, info);
    PageSetLSN(BufferGetPage(buffer), recptr);
}
```