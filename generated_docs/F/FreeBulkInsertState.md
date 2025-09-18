# FreeBulkInsertState

## Location
src/backend/access/heap/heapam.c: 1988 - 1999

## Overview
FreeBulkInsertState cleans up and deallocates a BulkInsertState object after bulk insert operations are completed, releasing associated buffers and memory.

## Definition
```c
void FreeBulkInsertState(BulkInsertState bistate);
```

## Detailed Description
FreeBulkInsertState performs cleanup operations for a BulkInsertState object that was previously created by GetBulkInsertState. The function ensures proper resource deallocation by releasing any currently held buffer, freeing the associated access strategy, and deallocating the state structure itself. This function is essential for preventing memory leaks and buffer pool exhaustion after bulk insert operations.

The cleanup process involves three key steps: releasing any active buffer that may still be pinned, freeing the bulk write access strategy, and deallocating the state structure memory.

## Parameters / Member Variables
- `bistate`: The BulkInsertState object to be freed and cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - ReleaseBuffer
  - FreeAccessStrategy
  - pfree
  - InvalidBuffer (for comparison)
- Called from (representative examples):
  - CopyMultiInsertBufferCleanup
  - CopyFrom
  - intorel_shutdown
  - transientrel_shutdown
  - ATRewriteTable

## Notes and Other Information
- Must be called for every BulkInsertState created with GetBulkInsertState to avoid memory leaks
- Safely handles the case where current_buf is InvalidBuffer (no buffer currently held)
- The function releases both the buffer resource and the access strategy before freeing the structure
- Typically called in cleanup paths and error handling routines to ensure proper resource management