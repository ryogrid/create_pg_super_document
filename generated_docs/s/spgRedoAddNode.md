# spgRedoAddNode

## Location
[src/backend/access/spgist/spgxlog.c:284-450](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgxlog.c#L284-L450)

## Overview
Replays the addition of an inner node to an SP-GiST index during WAL recovery, handling both in-place updates and cross-page moves with proper redirection and parent link maintenance.

## Definition
```c
static void spgRedoAddNode(XLogReaderState *record)
```

## Detailed Description
This function handles the WAL replay of SP-GiST inner node addition operations, which can occur in two distinct scenarios:

**In-place Update (single page)**:
- When there is no second block reference, the operation updates the inner tuple in place
- Simply deletes the old tuple and adds the new one at the same offset

**Cross-page Move (multiple pages)**:
- When a second block reference exists, the operation moves the inner tuple to a new location
- Follows a three-phase approach for consistency:
  1. **Install new tuple**: Adds the inner tuple to the destination page first
  2. **Replace old tuple**: Removes the original tuple and replaces it with a redirect/placeholder
  3. **Update parent links**: Updates parent downlinks to point to the new location

The function handles different parentBlk values (0, 1, 2) to optimize parent updates by doing them inline when the parent is on the same page being updated.

## Parameters / Member Variables
- `record`: XLogReaderState containing the WAL record data for the add node operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData (extract WAL record data)
  - XLogRecHasBlockRef (check for multiple block references)
  - [fillFakeState](../f/fillFakeState.md) (initialize minimal SP-GiST state)
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md) (get block numbers)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md) (read existing buffers)
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md) (initialize new buffer)
  - [SpGistInitBuffer](../S/SpGistInitBuffer.md) (initialize SP-GiST page)
  - [BufferGetPage](../B/BufferGetPage.md) (get page from buffer)
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md) (delete old tuple)
  - PageAddItem (add new tuple)
  - [addOrReplaceTuple](../a/addOrReplaceTuple.md) (add tuple to page)
  - [spgFormDeadTuple](spgFormDeadTuple.md) (create redirect/placeholder tuple)
  - [PageGetItem](../P/PageGetItem.md), PageGetItemId (page item access)
  - [spgUpdateNodeLink](spgUpdateNodeLink.md) (update parent downlinks)
  - SpGistPageGetOpaque (get page-specific data)
  - [PageSetLSN](../P/PageSetLSN.md), MarkBufferDirty (page finalization)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md) (resource cleanup)
  - memcpy (copy unaligned tuple header)
- Called from (representative examples):
  - [spg_redo](spg_redo.md) (main SP-GiST WAL redo dispatcher)

## Notes and Other Information
- This is a static function used only within the SP-GiST WAL replay module (spgxlog.c)
- Handles both simple in-place updates and complex cross-page moves
- Uses proper ordering for cross-page moves: destination first, source second, parent third
- Optimizes parent link updates by doing them inline when possible (parentBlk = 0 or 1)
- Creates different dead tuple types based on isBuild flag (SPGIST_PLACEHOLDER vs SPGIST_REDIRECT)
- Maintains proper placeholder and redirection counters in page opaque data
- AddNode operations are not used for null-storing pages (SpGistInitBuffer called with flags = 0)
- Handles unaligned inner tuple data by copying header to aligned structure
- Critical for SP-GiST index structure modifications during node splits and reorganization
- Error checking ensures all tuple additions succeed or abort with elog(ERROR)
- The parentBlk field indicates which buffer contains the parent tuple (0=source, 1=dest, 2=separate page)