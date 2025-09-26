# ginRedoCreatePTree

## Location
[src/backend/access/gin/ginxlog.c:44-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginxlog.c#L44-L70)

## Overview
Creates and initializes a new posting tree (data leaf page) in a GIN index during WAL replay, setting up the page structure and copying the posting list data.

## Definition
```c
static void ginRedoCreatePTree(XLogReaderState *record)
```

## Detailed Description
This function is responsible for replaying the creation of a GIN posting tree during WAL recovery. A posting tree is a specialized data structure used in GIN indexes to store large posting lists efficiently. The function initializes a new buffer as a GIN data leaf page with compressed posting list data.

The function extracts the posting tree creation data from the WAL record, initializes the buffer with appropriate GIN page flags (GIN_DATA | GIN_LEAF | GIN_COMPRESSED), copies the posting list data to the page, sets the data size, and ensures WAL consistency by updating the page LSN.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record with posting tree creation information

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [GinInitBuffer](../G/GinInitBuffer.md)
  - GinDataLeafPageGetPostingList
  - GinDataPageSetDataSize
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Data structures used:
  - [ginxlogCreatePostingTree](ginxlogCreatePostingTree.md)
- Constants used:
  - GIN_DATA
  - GIN_LEAF
  - GIN_COMPRESSED
- Called from:
  - [gin_redo](gin_redo.md)

## Notes and Other Information
- This is a static function used exclusively in GIN WAL replay operations
- The function creates compressed posting tree pages, which are optimized for storage efficiency
- Posting trees are created when posting lists become too large to fit efficiently in regular entry tree pages
- The function handles the complete setup of a new data leaf page including proper flag initialization and data copying
- WAL consistency is maintained through proper LSN updates and buffer management