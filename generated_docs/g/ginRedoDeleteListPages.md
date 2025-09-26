# ginRedoDeleteListPages

## Location
[src/backend/access/gin/ginxlog.c:675-725](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginxlog.c#L675-L725)

## Overview
This function handles the redo operation for GIN list page deletions during WAL recovery, updating the metapage and re-initializing the deleted pages as empty, deleted pages.

## Definition
```c
static void ginRedoDeleteListPages(XLogReaderState *record)
```

## Detailed Description
The `ginRedoDeleteListPages` function is responsible for replaying GIN list page deletion operations during PostgreSQL's crash recovery process. It performs the following operations:

1. **Metapage Update**: First updates the GIN metapage by:
   - Re-initializing the metapage buffer
   - Copying the new metadata from the WAL record
   - Setting the LSN and marking it dirty

2. **Page Deletion Processing**: For each page to be deleted:
   - Initializes the buffer for redo
   - Re-initializes the page as a deleted page using `GinInitBuffer` with `GIN_DELETED` type
   - Sets the LSN and marks the buffer dirty

The function implements a specific locking strategy during replay that differs from normal operation. During replay, it locks pages one at a time rather than all simultaneously, which is safe because:
- Pages are deleted from the head of the list
- Readers share-lock the next page before releasing their current one
- New readers are blocked behind the metapage lock and see the fully updated page list

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the list page deletion operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [GinInitMetabuffer](../G/GinInitMetabuffer.md)
  - GinPageGetMeta
  - [GinInitBuffer](../G/GinInitBuffer.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)

- Called from:
  - [gin_redo](gin_redo.md)

## Notes and Other Information
- This is a static function used internally within the GIN WAL recovery system
- No full-page images are taken of deleted pages; they are simply re-initialized as empty deleted pages
- The locking strategy during replay is optimized and differs from normal operation for safety
- Right-links of deleted pages don't need to be preserved since no new readers can access them
- The function handles bulk deletion of multiple pages in a single operation
- Located in src/backend/access/gin/ginxlog.c:675-725