# spgRedoMoveLeafs

## Location
src/backend/access/spgist/spgxlog.c: 171 - 283

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
  - XLogRecGetBlockTag (get destination block number)
  - fillFakeState (initialize minimal SP-GiST state)
  - XLogInitBufferForRedo (initialize new destination buffer)
  - SpGistInitBuffer (initialize SP-GiST page)
  - XLogReadBufferForRedo (read existing buffers)
  - BufferGetPage (get page from buffer)
  - memcpy (copy unaligned tuple headers)
  - addOrReplaceTuple (insert tuples on destination)
  - spgPageIndexMultiDelete (delete tuples and add redirections)
  - PageGetItem, PageGetItemId (page item access)
  - spgUpdateNodeLink (update parent downlinks)
  - PageSetLSN, MarkBufferDirty (page finalization)
  - UnlockReleaseBuffer (resource cleanup)
  - SizeOfSpgxlogMoveLeafs, SpGistLeafTupleData (data structures)
  - SPGIST_LEAF, SPGIST_NULLS, SPGIST_REDIRECT, SPGIST_PLACEHOLDER (flags)
- Called from (representative examples):
  - spg_redo (main SP-GiST WAL redo dispatcher)

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