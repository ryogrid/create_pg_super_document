# ginRedoVacuumDataLeafPage

## Location
[src/backend/access/gin/ginxlog.c:452-476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginxlog.c#L452-L476)

## Overview
Replays GIN data leaf page vacuum operations during WAL recovery, recompressing the page using stored compression data.

## Definition

```c
static void
ginRedoVacuumDataLeafPage(XLogReaderState *record)
```
## Detailed Description
ginRedoVacuumDataLeafPage is a WAL recovery function that replays GIN (Generalized Inverted Index) data leaf page vacuum operations from transaction log records. This function specifically handles the recovery of vacuum operations on data leaf pages, which store the actual posting lists (item pointer arrays) in compressed form.

Unlike the general ginRedoVacuumPage function that works with full-page images, this function performs incremental recovery by recompressing the page using compression data stored in the WAL record. This approach is more efficient for data leaf pages because vacuum operations on these pages primarily involve reorganizing and recompressing posting lists rather than wholesale page reorganization.

Key functionality:
- Validates that the target page is indeed a data leaf page
- Extracts vacuum compression data from the WAL record
- Recompresses the page using the ginRedoRecompress function
- Updates the page LSN and marks the buffer dirty for consistency

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record being replayed, including compression data for the vacuumed data leaf page

## Dependencies
- Functions called/Symbols referenced:
  - XLogReadBufferForRedo
  - [BufferGetPage](../B/BufferGetPage.md)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - GinPageIsLeaf
  - GinPageIsData
  - [ginRedoRecompress](ginRedoRecompress.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - MarkBufferDirty
  - [BufferIsValid](../B/BufferIsValid.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)

- Data structures used:
  - [ginxlogVacuumDataLeafPage](ginxlogVacuumDataLeafPage.md)

- Constants used:
  - BLK_NEEDS_REDO

- Called from:
  - [gin_redo](gin_redo.md)

## Notes and Other Information
- Specifically designed for GIN data leaf pages (as opposed to entry tree pages)
- Uses incremental recovery with recompression rather than full-page images
- Includes assertions to verify the page is both a leaf page and a data page
- The ginxlogVacuumDataLeafPage structure contains compression data needed for page reconstruction
- More efficient than full-page image approach for data leaf pages due to their compressed nature
- Part of PostgreSQL's GIN index vacuum recovery mechanism for posting list maintenance