# hash_xlog_delete

## Location
src/backend/access/hash/hash_xlog.c: 861 - 938

## Overview
Replays a hash index delete operation during WAL recovery, removing index tuples from bucket or overflow pages while maintaining proper locking.

## Definition
static void hash_xlog_delete(XLogReaderState *record)

## Detailed Description
This function handles the replay of tuple deletion operations in hash indexes during PostgreSQL's crash recovery process. The function manages two buffers: a primary bucket buffer (for cleanup locking) and a delete buffer (the page from which tuples are being removed). It uses careful locking protocol to ensure that concurrent scans don't experience inconsistencies during replay. The function removes the specified tuples using PageIndexMultiDelete and optionally clears the dead tuple marking flag if requested. This operation is part of hash index maintenance operations like VACUUM or tuple cleanup.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the delete operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogReadBufferForRedoExtended
  - XLogReadBufferForRedo
  - XLogRecGetBlockData
  - BufferGetPage
  - PageIndexMultiDelete
  - HashPageGetOpaque
  - PageSetLSN
  - MarkBufferDirty
  - BufferIsValid
  - UnlockReleaseBuffer
  - xl_hash_delete
  - XLogRedoAction
  - HashPageOpaque
  - RBM_NORMAL
  - BLK_NEEDS_REDO
  - LH_PAGE_HAS_DEAD_TUPLES
- Called from (representative examples):
  - hash_redo

## Notes and Other Information
- This is a static function used only within the hash WAL recovery module
- Implements cleanup locking protocol to prevent concurrent scan issues during replay
- Conditionally clears the LH_PAGE_HAS_DEAD_TUPLES flag based on the clear_dead_marking field in the WAL record
- Handles both primary bucket page deletions and overflow page deletions
- Part of PostgreSQL's hash index maintenance WAL recovery infrastructure
- The function ensures proper buffer management with validity checks and proper unlock/release order
- Related to hashbucketcleanup() operations for maintaining hash index integrity
- Supports bulk deletion of multiple tuples in a single operation through PageIndexMultiDelete