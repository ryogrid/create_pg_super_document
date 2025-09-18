# CopyMultiInsertBufferCleanup

## Location
src/backend/commands/copyfrom.c: 478 - 519

## Overview
Cleans up and deallocates a CopyMultiInsertBuffer after flushing, releasing all associated resources including tuple slots, bulk insert state, and the buffer structure itself.

## Definition
```c
static inline void CopyMultiInsertBufferCleanup(CopyMultiInsertInfo *miinfo,
                                               CopyMultiInsertBuffer *buffer)
```

## Detailed Description
This function performs comprehensive cleanup of a CopyMultiInsertBuffer structure that has been flushed. It ensures proper resource deallocation by:

1. **Validation**: Verifying the buffer was properly flushed (nused == 0) before cleanup
2. **Back-link Removal**: Clearing the ri_CopyMultiInsertBuffer reference in the ResultRelInfo to prevent dangling pointers
3. **State Cleanup**: For regular tables, freeing the BulkInsertState using FreeBulkInsertState
4. **Slot Cleanup**: Dropping all non-null TupleTableSlots up to MAX_BUFFERED_TUPLES using ExecDropSingleTupleTableSlot
5. **Bulk Insert Finalization**: For regular tables, calling table_finish_bulk_insert to complete the bulk operation
6. **Memory Deallocation**: Freeing the buffer structure itself

The function handles both regular tables (with BulkInsertState) and foreign tables (without BulkInsertState) appropriately.

## Parameters / Member Variables
- `miinfo`: Pointer to CopyMultiInsertInfo containing copy operation context and table insert options
- `buffer`: Pointer to CopyMultiInsertBuffer to be cleaned up (must be already flushed)

## Dependencies
- Functions called/Symbols referenced:
  - FreeBulkInsertState (cleanup of bulk insert state)
  - MAX_BUFFERED_TUPLES (constant defining maximum buffered tuples)
  - ExecDropSingleTupleTableSlot (slot cleanup)
  - table_finish_bulk_insert (bulk insert finalization)
- Called from (representative examples):
  - CopyMultiInsertInfoFlush (at src/backend/commands/copyfrom.c:558)
  - CopyMultiInsertInfoCleanup (at src/backend/commands/copyfrom.c:572)

## Notes and Other Information
The function includes assertions to ensure proper usage - the buffer must be flushed (nused == 0) before cleanup. It creates slots on demand during normal operation, so cleanup only needs to handle non-null slots. The cleanup is essential for preventing memory leaks and ensuring proper resource management during COPY operations, especially when dealing with partitioned tables that may have multiple buffers.