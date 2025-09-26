# XLogReadBufferForRedoExtended

## Location
src/backend/access/transam/xlogutils.c: 351 - 470

## Overview
Extended version of XLogReadBufferForRedo that provides additional control over buffer reading modes and locking behavior during WAL replay, supporting page extension and specialized locking modes.

## Definition

```c
XLogRedoAction
XLogReadBufferForRedoExtended(XLogReaderState *record,
							  uint8 block_id,
							  ReadBufferMode mode, bool get_cleanup_lock,
							  Buffer *buf)
```
## Detailed Description
This is the comprehensive implementation underlying both XLogReadBufferForRedo and XLogInitBufferForRedo. It handles all aspects of reading buffers during WAL replay with fine-grained control over behavior.

Key functionality includes:
- **Full-page image restoration**: When a WAL record contains a full-page image with BKPIMAGE_APPLY flag, the page is restored completely
- **LSN-based replay logic**: Compares record EndRecPtr with page LSN to determine if replay is needed
- **Page extension**: In RBM_ZERO_* modes, extends relations with zero-filled pages if the target block doesn't exist
- **Validation**: Ensures WILL_INIT flag consistency between WAL record and buffer mode
- **Special handling**: INIT_FORKNUM buffers are force-flushed to maintain disk synchronization

The function returns different XLogRedoAction values indicating what action the caller should take based on the page state and WAL record content.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record being processed
- : ID number identifying which block from the WAL record to read
- : ReadBufferMode specifying buffer access behavior (RBM_NORMAL, RBM_ZERO_AND_LOCK, etc.)
- : If true, acquires cleanup lock instead of regular exclusive lock
- : Output parameter receiving the buffer containing the requested page

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetBlockTagExtended
  - XLogRecBlockImageApply
  - XLogReadBufferExtended
  - RestoreBlockImage
  - PageSetLSN, PageGetLSN, PageIsNew
  - LockBufferForCleanup
  - FlushOneBuffer
- Constants used:
  - BLK_NEEDS_REDO, BLK_DONE, BLK_RESTORED, BLK_NOTFOUND
  - BKPBLOCK_WILL_INIT
  - RBM_ZERO_AND_LOCK, RBM_ZERO_AND_CLEANUP_LOCK
- Called from:
  - XLogReadBufferForRedo (simplified wrapper)
  - XLogInitBufferForRedo (initialization wrapper)
  - Various specialized redo functions requiring extended control

## Notes and Other Information
- This is the core implementation for all WAL buffer reading operations
- Enforces strict consistency between WAL record flags and requested buffer modes
- Full-page images always take precedence over existing page content for data integrity
- INIT_FORKNUM special handling ensures unlogged relation consistency
- Cleanup locks provide stronger concurrency control for vacuum-related operations
- The function handles all error conditions with appropriate PANIC messages for data integrity violations