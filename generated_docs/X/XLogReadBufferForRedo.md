# XLogReadBufferForRedo

## Location
src/backend/access/transam/xlogutils.c: 314 - 325

## Overview
Reads a page during WAL replay and determines what action needs to be taken to redo changes, serving as the primary interface for buffer access during crash recovery and hot standby.

## Definition

```c
XLogRedoAction
XLogReadBufferForRedo(XLogReaderState *record, uint8 block_id,
					  Buffer *buf)
```
## Detailed Description
This function is a simplified wrapper around  that reads a block referenced by a WAL record into the shared buffer cache during replay. It determines the appropriate redo action by comparing the record's EndRecPtr with the page's LSN. If the WAL record includes a full-page image, it is automatically restored.

The function handles several scenarios:
- Restoring full-page images when available with BKPIMAGE_APPLY flag
- Detecting if changes have already been applied by LSN comparison
- Managing truncated pages that no longer exist
- Ensuring proper buffer locking for subsequent operations

The returned buffer is always locked in exclusive mode, even when no replay is needed, to satisfy requirements of functions like MarkBufferDirty and to maintain consistency in hot standby mode.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record being replayed
- : ID number identifying which block from the WAL record to process
- : Output parameter receiving the locked buffer containing the requested page

## Dependencies
- Functions called/Symbols referenced:
  - XLogReadBufferForRedoExtended
  - RBM_NORMAL (buffer read mode constant)
- Called from (representative examples):
  - heap_xlog_insert (src/backend/access/heap/heapam.c:9646)
  - btree_xlog_insert (src/backend/access/nbtree/nbtxlog.c:179)
  - gin_redo operations (src/backend/access/gin/ginxlog.c)
  - Various index-specific redo functions across access methods

## Notes and Other Information
- This is the standard interface for WAL replay - most redo functions use this rather than the extended version
- Always uses RBM_NORMAL buffer read mode and does not trigger page extension
- Full-page images are trusted over database pages due to CRC validation
- The function prioritizes data integrity by replaying all subsequent WAL modifications when full-page images are restored
- Buffer remains locked after return to prevent concurrent modifications during redo processing