# hash_xlog_split_cleanup

## Location
[src/backend/access/hash/hash_xlog.c:939-963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash_xlog.c#L939-L963)

## Overview
This function replays the split cleanup flag operation for a primary bucket page during PostgreSQL hash index WAL recovery, clearing the flag that indicates a bucket requires split cleanup.

## Definition


## Detailed Description
The hash_xlog_split_cleanup function is a WAL replay handler that processes split cleanup operations during crash recovery for hash indexes. When a hash bucket is split, the original bucket page is marked with the LH_BUCKET_NEEDS_SPLIT_CLEANUP flag to indicate it requires cleanup. This function replays the operation that clears this flag, indicating the split cleanup has been completed.

During replay, the function reads the buffer specified in the WAL record, checks if redo is needed, and if so, removes the LH_BUCKET_NEEDS_SPLIT_CLEANUP flag from the bucket page's opaque data structure. This ensures the page state is correctly restored during recovery.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record being replayed, including buffer references and LSN information

## Dependencies
- Functions called/Symbols referenced:
  - XLogReadBufferForRedo
  - [BufferGetPage](../B/BufferGetPage.md)
  - HashPageGetOpaque
  - [PageSetLSN](../P/PageSetLSN.md)
  - MarkBufferDirty
  - [BufferIsValid](../B/BufferIsValid.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Types referenced:
  - HashPageOpaque
  - BLK_NEEDS_REDO
  - LH_BUCKET_NEEDS_SPLIT_CLEANUP
- Called from:
  - [hash_redo](hash_redo.md)

## Notes and Other Information
- This is a static function used only within the hash WAL recovery subsystem
- The function follows the standard pattern for WAL replay handlers: read buffer, check if redo needed, apply changes, update LSN, mark dirty
- The LH_BUCKET_NEEDS_SPLIT_CLEANUP flag is part of the hash index split mechanism to track which buckets need post-split cleanup
- Buffer management (lock/unlock) is handled properly to avoid resource leaks during recovery