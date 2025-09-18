# hash_xlog_squeeze_page

## Location
src/backend/access/hash/hash_xlog.c: 627 - 860

## Overview
Replays a hash index squeeze operation during WAL recovery, consolidating overflow pages and updating multiple related pages including bucket, overflow, bitmap, and meta pages.

## Definition
static void hash_xlog_squeeze_page(XLogReaderState *record)

## Detailed Description
This function handles the replay of a complete hash index squeeze operation during PostgreSQL's crash recovery process. A squeeze operation removes an overflow page by moving its contents to other pages and updating all related page linkages. The operation is complex and involves multiple buffers: primary bucket page (for locking), write buffer (destination for moved tuples), overflow buffer (page being freed), previous buffer (page before the freed page), next buffer (page after the freed page), bitmap buffer (tracks free pages), and meta buffer (index metadata). The function ensures proper locking order, moves tuples when necessary, initializes the freed page as unused, updates page linkages, marks the page as free in the bitmap, and updates the metadata to track the newly available overflow page.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the squeeze operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogReadBufferForRedoExtended
  - XLogReadBufferForRedo
  - XLogRecGetBlockData
  - XLogRecHasBlockRef
  - BufferGetPage
  - BufferGetPageSize
  - IndexTupleSize
  - PageAddItem
  - HashPageGetOpaque
  - HashPageGetBitmap
  - HashPageGetMeta
  - _hash_pageinit
  - PageSetLSN
  - MarkBufferDirty
  - BufferIsValid
  - UnlockReleaseBuffer
  - CLRBIT
  - xl_hash_squeeze_page
  - XLogRedoAction
  - HashPageOpaque
  - HashMetaPage
  - RBM_NORMAL
  - BLK_NEEDS_REDO
  - BLK_NOTFOUND
  - InvalidOffsetNumber
  - InvalidBucket
  - LH_UNUSED_PAGE
  - HASHO_PAGE_ID
  - Item
- Called from (representative examples):
  - hash_redo

## Notes and Other Information
- This is a static function used only within the hash WAL recovery module
- Most complex of the hash WAL recovery functions, handling up to 7 different buffers
- Implements careful locking protocol to prevent concurrent scan issues during replay
- Updates multiple data structures: page contents, page linkages, free space bitmap, and metadata
- Handles conditional logic based on whether pages are the same (optimization for common cases)
- Part of PostgreSQL's hash index space reclamation WAL recovery infrastructure
- Includes extensive assertion checks and error handling for data integrity
- Buffer release order is carefully managed to maintain proper lock hierarchy
- During replay, bitmap and meta page updates don't require holding locks on other pages since no concurrent updates can occur