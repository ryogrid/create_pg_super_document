# hash_xlog_split_complete

## Location
[src/backend/access/hash/hash_xlog.c:442-500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash_xlog.c#L442-L500)

## Overview
Replays the completion phase of a hash index split operation during WAL recovery, updating bucket flags on both old and new bucket pages.

## Definition
static void hash_xlog_split_complete(XLogReaderState *record)

## Detailed Description
This function handles the replay of the completion phase of a hash index split operation during PostgreSQL's crash recovery process. Unlike hash_xlog_split_page which handles the initial split, this function focuses on finalizing the split by updating the bucket flags on both the old and new bucket pages. The function processes two buffers - one for the old bucket and one for the new bucket - and sets their respective hasho_flag values based on the information stored in the WAL record. Even when pages are restored from full-page images, the bucket flags still need to be updated since they are not included in the page images.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the split completion operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - HashPageGetOpaque
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [BufferIsValid](../B/BufferIsValid.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [xl_hash_split_complete](../x/xl_hash_split_complete.md)
  - XLogRedoAction
  - HashPageOpaque
  - BLK_NEEDS_REDO
  - BLK_RESTORED
- Called from (representative examples):
  - [hash_redo](hash_redo.md)

## Notes and Other Information
- This is a static function used only within the hash WAL recovery module
- Processes both old and new bucket pages in sequence
- Updates bucket flags even when pages are restored from full-page images, as flags are not included in the images
- Part of PostgreSQL's hash index WAL recovery infrastructure for split operations
- The function handles buffer validity checks to ensure proper resource management