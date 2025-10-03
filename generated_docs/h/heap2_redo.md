# heap2_redo

## Location
[src/backend/access/heap/heapam.c:10384-10422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L10384-L10422)

## Overview
WAL redo function for advanced heap operations that require conflict processing during recovery, handling operations like pruning, visibility map updates, and multi-inserts.

## Definition
```c
void heap2_redo(XLogReaderState *record)
```

## Detailed Description
The `heap2_redo` function complements `heap_redo` by handling more complex heap operations that may require MVCC conflict processing during recovery. This function is part of PostgreSQL's dual heap resource manager approach, where heap2 handles operations that can affect transaction visibility and require special handling during hot standby recovery.

The function processes various advanced heap operations including page pruning (from vacuum operations), visibility map updates, multi-tuple inserts, lock updates, and logical rewrite operations. Some operations like NEW_CID are no-ops during physical recovery as they're only needed for logical decoding.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record with operation type and data for the heap2 operation to be replayed

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo (extracts operation info from WAL record)
  - [heap_xlog_prune_freeze](heap_xlog_prune_freeze.md) (handles pruning and freezing operations)
  - [heap_xlog_visible](heap_xlog_visible.md) (handles visibility map updates)
  - [heap_xlog_multi_insert](heap_xlog_multi_insert.md) (handles multi-tuple insert operations)
  - [heap_xlog_lock_updated](heap_xlog_lock_updated.md) (handles lock update operations)
  - [heap_xlog_logical_rewrite](heap_xlog_logical_rewrite.md) (handles logical rewrite operations)
- Called from:
  - WAL replay infrastructure (not directly referenced by other functions)

## Notes and Other Information
- Handles operations that may require MVCC conflict processing, unlike basic heap_redo operations
- NEW_CID operations are no-ops during physical recovery, only used for logical decoding
- Part of PostgreSQL's dual resource manager approach for heap operations
- Critical for hot standby and streaming replication functionality
- Will panic if encountering unknown operation codes
- Operations handled here often relate to vacuum, visibility, and bulk operations

## Simplified Source

```c
void heap2_redo(XLogReaderState *record) {
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    // Dispatch to handlers for advanced heap operations that may require
    // MVCC conflict processing during recovery
    switch (info & XLOG_HEAP_OPMASK) {
        case XLOG_HEAP2_PRUNE_ON_ACCESS:
        case XLOG_HEAP2_PRUNE_VACUUM_SCAN:
        case XLOG_HEAP2_PRUNE_VACUUM_CLEANUP:
            // Handle page pruning and freezing operations
            heap_xlog_prune_freeze(record);
            break;
        case XLOG_HEAP2_VISIBLE:
            // Handle visibility map updates
            heap_xlog_visible(record);
            break;
        case XLOG_HEAP2_MULTI_INSERT:
            // Handle bulk insert operations
            heap_xlog_multi_insert(record);
            break;
        case XLOG_HEAP2_LOCK_UPDATED:
            // Handle lock update operations
            heap_xlog_lock_updated(record);
            break;
        case XLOG_HEAP2_NEW_CID:
            // No-op during physical recovery
            // Only used for logical decoding
            break;
        case XLOG_HEAP2_REWRITE:
            // Handle logical rewrite operations
            heap_xlog_logical_rewrite(record);
            break;
        default:
            elog(PANIC, "heap2_redo: unknown op code %u", info);
    }
}
```