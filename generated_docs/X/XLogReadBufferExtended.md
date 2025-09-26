# XLogReadBufferExtended

## Location
src/backend/access/transam/xlogutils.c: 471 - 562

## Overview
Low-level function that reads pages during WAL replay with extended functionality, handling buffer caching, relation extension, and invalid page detection with various buffer reading modes.

## Definition


## Detailed Description
This function provides the core buffer reading functionality during WAL replay, comparable to ReadBufferExtended but with specialized behavior for recovery scenarios. It handles multiple reading modes with different behaviors for missing or invalid pages.

Key behaviors by mode:
- **RBM_NORMAL**: Returns InvalidBuffer for non-existent or all-zero pages, with invalid page logging
- **RBM_ZERO_***: Extends relations with zero-filled pages when blocks don't exist
- **RBM_NORMAL_NO_LOG**: Returns InvalidBuffer for missing pages without logging

The function includes optimizations for buffer cache hits via recent_buffer hints and ensures proper relation creation during replay. It validates page initialization and logs invalid pages for consistency checking.

Important: Redo functions should typically use XLogReadBufferForRedoExtended instead of calling this directly, as it ensures proper WAL record registration for page modifications.

## Parameters / Member Variables
- : RelFileLocator identifying the target relation
- : Fork number (main, FSM, VM, or init fork)
- : Block number within the fork to read
- : ReadBufferMode specifying read behavior and extension policy
- : Hint buffer that might contain the target page for optimization

## Dependencies
- Functions called/Symbols referenced:
  - ReadRecentBuffer
  - smgropen, smgrcreate, smgrnblocks
  - ReadBufferWithoutRelcache
  - ExtendBufferedRelTo
  - log_invalid_page
  - PageIsNew, ReleaseBuffer
- Constants used:
  - RBM_NORMAL, RBM_NORMAL_NO_LOG, RBM_ZERO_*
  - INVALID_PROC_NUMBER
  - EB_PERFORMING_RECOVERY, EB_SKIP_EXTENSION_LOCK
- Called from:
  - XLogReadBufferForRedoExtended (primary caller)
  - verifyBackupPageConsistency
  - XLogRecordPageWithFreeSpace

## Notes and Other Information
- Creates target files automatically to handle replay sequences with later-deleted relations
- Uses recent_buffer hints to optimize buffer pool lookups
- Extends relations during recovery without requiring extension locks
- Validates page initialization in RBM_NORMAL mode to detect corruption
- Invalid page logging supports end-of-recovery consistency verification
- The function assumes single-process recovery environment for some optimizations
- Proper error handling ensures data integrity during replay operations