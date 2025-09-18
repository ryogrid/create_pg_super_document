# hash_xlog_move_page_contents

## Location
src/backend/access/hash/hash_xlog.c: 501 - 626

## Overview
Replays the movement of page contents during a hash index squeeze operation, transferring index tuples from one page to another while maintaining proper locking.

## Definition
static void hash_xlog_move_page_contents(XLogReaderState *record)

## Detailed Description
This function handles the replay of page content movement during hash index squeeze operations. A squeeze operation occurs when hash index pages need to be consolidated to reclaim space. The function manages three buffers: a primary bucket buffer (for locking), a write buffer (destination for moved tuples), and a delete buffer (source of tuples to be moved). The operation requires careful coordination to ensure that concurrent scans don't miss records or see duplicates. The function first acquires cleanup locks, then adds tuples to the destination page, removes them from the source page, and finally releases all buffers in the proper order.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the page content movement operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogReadBufferForRedoExtended
  - XLogReadBufferForRedo
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - IndexTupleSize
  - PageAddItem
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - MarkBufferDirty
  - [BufferIsValid](../B/BufferIsValid.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [xl_hash_move_page_contents](../x/xl_hash_move_page_contents.md)
  - XLogRedoAction
  - RBM_NORMAL
  - BLK_NEEDS_REDO
  - InvalidOffsetNumber
  - Item
- Called from (representative examples):
  - [hash_redo](hash_redo.md)

## Notes and Other Information
- This is a static function used only within the hash WAL recovery module
- Implements careful locking protocol to prevent concurrent scan issues during replay
- Handles both addition of tuples to destination page and deletion from source page
- Uses cleanup locks on primary bucket page to ensure exclusive access during operation
- Part of PostgreSQL's hash index squeeze operation WAL recovery infrastructure
- Includes assertion checks to verify that the number of inserted tuples matches the WAL record expectations
- Buffer management is done in specific order to maintain lock hierarchy and prevent deadlocks