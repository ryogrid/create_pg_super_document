# heap_insert

## Location
src/backend/access/heap/heapam.c: 2038 - 2228

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
  - heap_prepare_insert
  - GetCurrentTransactionId
  - RelationGetBufferForTuple
  - CheckForSerializableConflictIn
  - RelationPutHeapTuple
  - visibilitymap_clear
  - XLogInsert (and related WAL functions)
  - CacheInvalidateHeapTuple
  - pgstat_count_heap_insert
  - heap_freetuple
- Called from (representative examples):
  - simple_heap_insert
  - heapam_tuple_insert
  - heapam_tuple_insert_speculative
  - toast_save_datum

## Notes and Other Information
- Updates tup->t_self with the actual TID where the tuple was stored
- Handles both regular and speculative insertions (for unique constraint checking)
- Manages visibility map updates when inserting into all-visible pages
- Includes comprehensive WAL logging for crash recovery and replication
- Supports bulk insert optimization through BulkInsertState parameter
- Toasted field values are not reflected back into the original tuple structure
- Critical sections ensure atomicity of buffer modifications and WAL logging
- Includes logic for logical decoding support in replication scenarios