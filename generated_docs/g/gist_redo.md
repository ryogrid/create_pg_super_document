# gist_redo

## Location
src/backend/access/gist/gistxlog.c: 397 - 437

## Overview
Main WAL redo dispatcher function for GiST index operations, routing different WAL record types to their appropriate replay handlers.

## Definition
```c
void gist_redo(XLogReaderState *record)
```

## Detailed Description
This function serves as the central dispatcher for all GiST index WAL record replay operations during database recovery. It examines the WAL record type and delegates to the appropriate specialized redo function to restore the database state.

The function operates within a dedicated memory context (`opCtx`) to ensure proper memory management during recovery operations. It handles six different types of GiST WAL records:

1. **XLOG_GIST_PAGE_UPDATE**: Updates to existing pages (insertions, updates)
2. **XLOG_GIST_DELETE**: Tuple deletions
3. **XLOG_GIST_PAGE_REUSE**: Page reuse operations via FSM
4. **XLOG_GIST_PAGE_SPLIT**: Page split operations
5. **XLOG_GIST_PAGE_DELETE**: Page deletion operations
6. **XLOG_GIST_ASSIGN_LSN**: LSN assignment (no-op during replay)

The function includes a comment noting that GiST indexes do not require conflict processing, unlike some other index types, but reserves the possibility for future optimization similar to B-tree killed tuple removal.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record to be replayed

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo: Extract record type information
  - XLR_INFO_MASK: Mask for extracting record type bits
  - gistRedoPageUpdateRecord: Handle page update operations
  - gistRedoDeleteRecord: Handle tuple deletion operations
  - gistRedoPageReuse: Handle page reuse conflict resolution
  - gistRedoPageSplitRecord: Handle page split operations
  - gistRedoPageDelete: Handle page deletion operations
  - MemoryContextSwitchTo: Switch to operation memory context
  - MemoryContextReset: Reset memory context after operation
  - elog: Log error messages
- Called from:
  - WAL replay infrastructure (registered as GiST redo manager)

## Notes and Other Information
- This function is registered with the WAL replay system as the redo manager for GiST operations
- Uses a dedicated memory context (`opCtx`) that is reset after each operation to prevent memory leaks
- The `XLOG_GIST_ASSIGN_LSN` case is a no-op during replay as it only serves to assign fake LSNs during normal operation
- Unknown operation codes trigger a PANIC to indicate serious corruption or version mismatch
- The function explicitly notes that conflict processing is not required for GiST indexes
- Future optimizations similar to B-tree killed tuple removal would require adding conflict handling here