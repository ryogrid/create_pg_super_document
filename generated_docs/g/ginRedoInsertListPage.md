# ginRedoInsertListPage

## Location
src/backend/access/gin/ginxlog.c: 620 - 674

## Overview
This function handles the redo operation for GIN list page insertions during WAL recovery, re-initializing a list page and adding all tuples from the WAL record data.

## Definition
```c
static void ginRedoInsertListPage(XLogReaderState *record)
```

## Detailed Description
The `ginRedoInsertListPage` function is responsible for replaying GIN list page insertion operations during PostgreSQL's crash recovery process. It performs the following operations:

1. **Page Re-initialization**: Always completely re-initializes the target page as a GIN list page using `GinInitBuffer` with `GIN_LIST` type.

2. **Rightlink Configuration**: Sets up the page's rightlink pointer based on the WAL record data:
   - If rightlink is `InvalidBlockNumber`, this is the tail of a sublist, so it sets the page as full row and sets maxoff to 1
   - Otherwise, it's a regular list page with maxoff set to 0

3. **Tuple Insertion**: Iterates through all tuples in the WAL record payload and adds each one to the page:
   - Extracts tuples from the block data
   - Adds each tuple to the page using `PageAddItem`
   - Advances the offset number for each insertion

The function ensures proper page structure and tuple placement for GIN list pages during recovery.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the list page insertion operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogInitBufferForRedo
  - BufferGetPage
  - GinInitBuffer
  - GinPageGetOpaque
  - GinPageSetFullRow
  - XLogRecGetBlockData
  - IndexTupleSize
  - PageAddItem
  - PageSetLSN
  - MarkBufferDirty
  - UnlockReleaseBuffer

- Called from:
  - gin_redo

## Notes and Other Information
- This is a static function used internally within the GIN WAL recovery system
- The page is always completely re-initialized, ensuring a clean state during recovery
- Handles both tail pages (end of sublist) and intermediate list pages differently through rightlink and maxoff settings
- Proper error handling ensures that failed tuple insertions are reported
- Located in src/backend/access/gin/ginxlog.c:620-674