# ginRedoInsert

## Location
[src/backend/access/gin/ginxlog.c:347-401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginxlog.c#L347-L401)

## Overview
Replays GIN index insert operations during WAL recovery, handling both data and entry page insertions.

## Definition

```c
static void
ginRedoInsert(XLogReaderState *record)
```
## Detailed Description
ginRedoInsert is a WAL recovery function that replays GIN (Generalized Inverted Index) insert operations from transaction log records. It handles the restoration of insert operations on both data pages and entry pages within GIN indexes. The function processes different types of insertions based on flags in the WAL record, including leaf/non-leaf page distinctions and data/entry page types.

Key functionality includes:
- Processing incomplete split completion for non-leaf pages
- Determining insertion type (data vs entry pages, leaf vs non-leaf)
- Delegating to specialized insertion functions based on page type
- Proper LSN setting and buffer management for crash recovery consistency

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record being replayed, including insertion data and metadata

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [BlockIdGetBlockNumber](../B/BlockIdGetBlockNumber.md)  
  - [ginRedoClearIncompleteSplit](ginRedoClearIncompleteSplit.md)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - [ginRedoInsertData](ginRedoInsertData.md)
  - [ginRedoInsertEntry](ginRedoInsertEntry.md)
  - GinPageIsData
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)

- Called from:
  - [gin_redo](gin_redo.md)

## Notes and Other Information
- Uses ginxlogInsert structure to access WAL record data
- Handles both GIN_INSERT_ISLEAF and GIN_INSERT_ISDATA flags to determine processing path
- For non-leaf pages, extracts child block numbers and clears incomplete split flags
- Ensures proper crash recovery by setting LSN and marking buffers dirty
- Part of PostgreSQL's GIN index WAL recovery mechanism