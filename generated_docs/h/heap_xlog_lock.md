# heap_xlog_lock

## Location
src/backend/access/heap/heapam.c: 10166 - 10236

## Overview
Handles the replay of tuple locking operations during WAL recovery by updating tuple header information to reflect the lock state without modifying tuple data.

## Definition
```c
static void heap_xlog_lock(XLogReaderState *record)
```

## Detailed Description
The `heap_xlog_lock` function processes tuple locking operations during PostgreSQL's WAL recovery. This function is responsible for replaying locks that were placed on tuples during normal database operation. Unlike UPDATE operations, locking operations modify only the tuple header metadata (specifically the infomask fields and xmax) without changing the tuple's actual data content.

The function performs several key operations:
1. **Visibility map management**: Clears the "all frozen" bit in the visibility map if the lock operation affects tuple visibility
2. **Header updates**: Modifies the tuple's infomask and infomask2 fields to reflect the lock state
3. **Lock-only handling**: For lock-only operations (not updates), ensures the tuple's ctid points to itself and clears HOT update flags
4. **Transaction information**: Sets the appropriate xmax (locking transaction) and cmax values

The lock replay ensures that the tuple's visibility and locking state are correctly restored during recovery, maintaining consistency for concurrent access patterns.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record with lock operation details, including target tuple offset, lock flags, and transaction information

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData (extracts xl_heap_lock structure from WAL record)
  - XLogRecGetBlockTag (retrieves block information from WAL record)
  - XLogReadBufferForRedo (reads and locks target buffer for redo)
  - visibilitymap_pin, visibilitymap_clear (visibility map maintenance for frozen tuples)
  - PageGetMaxOffsetNumber, PageGetItemId, PageGetItem (page-level tuple access)
  - fix_infomask_from_infobits (reconstructs tuple visibility state from logged bits)
  - HeapTupleHeaderSetXmax, HeapTupleHeaderSetCmax (tuple header transaction info)
  - HEAP_XMAX_IS_LOCKED_ONLY (macro to check if operation is lock-only)
- Called from (representative examples):
  - heap_redo (main heap WAL replay dispatcher)

## Notes and Other Information
- **Lock-Only Operations**: Distinguishes between pure locking operations and lock-for-update operations using the HEAP_XMAX_IS_LOCKED_ONLY macro
- **Visibility Map Impact**: When locks affect tuple visibility (clearing frozen status), the visibility map must be updated accordingly
- **HOT Update Interaction**: For lock-only operations, clears HOT update flags and ensures self-referencing ctid to maintain tuple chain integrity
- **Transaction Consistency**: Properly sets xmax to the locking transaction and cmax for command ordering within transactions
- **Error Handling**: Includes PANIC-level validation to ensure tuple consistency during recovery
- **Metadata Focus**: Unlike update operations, this function only modifies tuple header metadata, not the tuple data itself