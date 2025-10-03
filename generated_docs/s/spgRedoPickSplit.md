# spgRedoPickSplit

## Location
[src/backend/access/spgist/spgxlog.c:529-750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgxlog.c#L529-L750)

## Overview
Replays a pick-split operation from the WAL for SP-GiST indexes, reconstructing the complex redistribution of tuples across multiple pages during crash recovery.

## Definition

```c
static void
spgRedoPickSplit(XLogReaderState *record)
```
## Detailed Description
This function handles the replay of SP-GiST pick-split operations, which are among the most complex WAL recovery operations in the SP-GiST access method. A pick-split occurs when an SP-GiST inner node becomes full and needs to redistribute its child tuples across multiple pages. The function:

1. Extracts comprehensive split information from the WAL record (tuples to delete, insert offsets, page selections)
2. Handles different split scenarios (root splits, source/destination page initialization)
3. Manages proper deletion of old tuples with redirection placeholder creation
4. Restores leaf tuples to appropriate source or destination pages
5. Creates new inner tuple and updates parent-child relationships
6. Updates parent downlinks to maintain tree consistency

The operation ensures atomicity and consistency during recovery by carefully ordering page updates and maintaining proper buffer locks.

## Parameters / Member Variables
- `*record`: XLogReaderState pointer containing the WAL record with pick-split operation details
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md)
  - XLogRecHasBlockRef
  - [fillFakeState](../f/fillFakeState.md)
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - [SpGistInitBuffer](../S/SpGistInitBuffer.md)
  - [spgPageIndexMultiDelete](spgPageIndexMultiDelete.md)
  - [addOrReplaceTuple](../a/addOrReplaceTuple.md)
  - [spgUpdateNodeLink](spgUpdateNodeLink.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Called from:
  - [spg_redo](spg_redo.md) (main SP-GiST redo dispatcher)

## Notes and Other Information
- Handles multiple complex scenarios: root splits, page initialization, tuple redistribution
- Manages unaligned tuple data by copying headers to properly aligned structures
- Supports both regular operations and index build scenarios with different deletion strategies
- Implements proper buffer management to avoid dangling references during Hot Standby
- Updates parent-child relationships and maintains tree structural integrity
- Part of the SP-GiST index WAL recovery subsystem located in src/backend/access/spgist/spgxlog.c:529-750
- One of the most sophisticated WAL recovery operations in PostgreSQL's SP-GiST implementation