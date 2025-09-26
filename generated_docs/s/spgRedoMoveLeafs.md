# spgRedoMoveLeafs

## Location
[src/backend/access/spgist/spgxlog.c:171-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgxlog.c#L171-L283)

## Overview
Replays the movement of leaf tuples from one SP-GiST page to another during WAL recovery, handling tuple deletion, insertion, redirection pointers, and parent link updates.

## Definition
```c
static void spgRedoMoveLeafs(XLogReaderState *record)
```

## Detailed Description
This function handles the WAL replay of SP-GiST leaf tuple movement operations, which typically occur during page splits or reorganization. The operation involves multiple coordinated steps:

1. **Setup**: Extracts WAL record data, initializes a fake SpGistState, and parses offset arrays for tuples to delete and insert
2. **Destination page handling**: Creates a new page or reads existing destination page, then inserts all moved leaf tuples using addOrReplaceTuple()
3. **Source page cleanup**: Deletes the original tuples from the source page and inserts redirection pointers (or placeholders during index build) to maintain tuple chain integrity
4. **Parent link updates**: Updates the parent inner tuple's downlink to point to the new destination page and offset

The function carefully handles unaligned tuple data and maintains proper ordering (destination first, then source, then parent) to ensure consistency during recovery.

## Parameters / Member Variables
- `record`: XLogReaderState containing the WAL record data for the move leafs operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData (extract WAL record data)
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md) (get destination block number)
  - [fillFakeState](../f/fillFakeState.md) (initialize minimal SP-GiST state)
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md) (initialize new destination buffer)
  - [SpGistInitBuffer](../S/SpGistInitBuffer.md) (initialize SP-GiST page)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md) (read existing buffers)
  - [BufferGetPage](../B/BufferGetPage.md) (get page from buffer)
  - memcpy (copy unaligned tuple headers)
  - [addOrReplaceTuple](../a/addOrReplaceTuple.md) (insert tuples on destination)
  - [spgPageIndexMultiDelete](spgPageIndexMultiDelete.md) (delete tuples and add redirections)
  - [PageGetItem](../P/PageGetItem.md), PageGetItemId (page item access)
  - [spgUpdateNodeLink](spgUpdateNodeLink.md) (update parent downlinks)
  - [PageSetLSN](../P/PageSetLSN.md), MarkBufferDirty (page finalization)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md) (resource cleanup)
  - SizeOfSpgxlogMoveLeafs, SpGistLeafTupleData (data structures)
  - SPGIST_LEAF, SPGIST_NULLS, SPGIST_REDIRECT, SPGIST_PLACEHOLDER (flags)
- Called from (representative examples):
  - [spg_redo](spg_redo.md) (main SP-GiST WAL redo dispatcher)

## Notes and Other Information
- This is a static function used only within the SP-GiST WAL replay module (spgxlog.c)
- Handles both new page creation (xldata->newPage) and moves to existing pages
- Maintains proper tuple chain integrity by using redirection pointers or placeholders
- Uses three-phase approach: destination page updates, source page cleanup, parent updates
- Special handling for index build operations (uses SPGIST_PLACEHOLDER instead of SPGIST_REDIRECT)
- Supports both regular and null-storing leaf pages via SPGIST_NULLS flag
- Handles unaligned tuple data by copying headers to aligned structures before access
- The replaceDead flag affects the number of tuples to insert (1 if replacing dead tuple, nMoves+1 otherwise)
- Critical for maintaining SP-GiST index consistency during page reorganization and splits
- Updates parent downlinks to point to the final destination tuple location