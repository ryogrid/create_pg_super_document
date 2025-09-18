# log_heap_prune_and_freeze

## Location
src/backend/access/heap/pruneheap.c: 2053 - 2172

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
  - XLogBeginInsert
  - XLogRegisterBuffer
  - heap_log_freeze_plan
  - XLogRegisterBufData
  - RelationIsAccessibleInLogicalDecoding
  - XLogRegisterData
  - XLogInsert
  - PageSetLSN
  - BufferGetPage
- Called from (representative examples):
  - heap_page_prune_and_freeze
  - lazy_vacuum_heap_page

## Notes and Other Information
This function is called within a critical section, so it must be efficient and avoid operations that could fail. The function destructively sorts the frozen tuples array through heap_log_freeze_plan. The unified record format allows for efficient WAL logging when multiple operations occur on the same page, which is common during vacuum operations. Different prune reasons result in different WAL record subtypes but use the same underlying record structure.