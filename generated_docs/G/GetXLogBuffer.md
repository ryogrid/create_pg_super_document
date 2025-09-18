# GetXLogBuffer

## Location
src/backend/access/transam/xlog.c: 1634 - 1749

## Overview
Returns a pointer to the appropriate location in the WAL buffer for a given XLogRecPtr, initializing the page if necessary.

## Definition
```c
static char *GetXLogBuffer(XLogRecPtr ptr, TimeLineID tli)
```

## Detailed Description
This function provides access to WAL buffer memory for writing WAL records at specific positions. It implements a sophisticated buffer management system with the following key features:

**Fast Path Optimization**: Uses static caching to quickly return pointers when accessing the same WAL page repeatedly, avoiding expensive buffer lookups.

**Buffer Organization**: The WAL buffer cache maps each WAL page to a specific buffer slot, allowing direct calculation of the required buffer from the XLogRecPtr alone.

**Page Initialization**: If a requested page isn't initialized, the function triggers initialization through `AdvanceXLInsertBuffer()`, which may involve evicting older buffers and performing I/O operations.

**Insertion Progress Tracking**: Before potentially blocking operations, the function updates the WAL insertion progress using `WALInsertLockUpdateInsertingAt()` to inform other processes about completed insertion positions.

**Memory Ordering**: Uses memory barriers to ensure proper visibility of page initialization and prevent race conditions where WAL data could be overwritten by delayed initialization.

The function handles special cases for page headers (both short and long formats) and ensures that advertised insertion positions don't lead to premature flushing of uninitialized pages.

## Parameters / Member Variables
- `ptr`: XLogRecPtr specifying the exact WAL position to access
- `tli`: TimeLineID indicating which timeline the WAL position belongs to

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtrToBufIdx (converts WAL position to buffer index)
  - pg_atomic_read_u64 (atomically reads buffer end positions)
  - WALInsertLockUpdateInsertingAt (updates insertion progress)
  - AdvanceXLInsertBuffer (initializes new WAL buffers)
  - pg_memory_barrier (ensures memory ordering)
  - XLogSegmentOffset (calculates offset within WAL segment)
- Called from (representative examples):
  - RefreshXLogWriteResult
  - CopyXLogRecordToWAL
  - CreateOverwriteContrecordRecord

## Notes and Other Information
- This is a static function, only accessible within the xlog.c module
- The caller must hold a WAL insertion lock with insertingAt ≤ ptr to prevent buffer eviction
- After calling this function, previously accessed buffers may be recycled
- Uses static variables for caching the most recently accessed page
- Critical for WAL record insertion performance and correctness
- Handles both short and long WAL page headers appropriately
- The function may block if buffer initialization or eviction is required