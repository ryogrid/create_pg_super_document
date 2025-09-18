# spgRedoSplitTuple

## Location
src/backend/access/spgist/spgxlog.c: 451 - 528

## Overview
Replays a split tuple operation from the WAL (Write-Ahead Log) for SP-GiST indexes, handling the reconstruction of prefix and postfix tuples during crash recovery.

## Definition


## Detailed Description
This function is part of the SP-GiST WAL recovery mechanism that replays split tuple operations. When an SP-GiST inner tuple is split during normal operation, this operation is logged to WAL. During recovery, this function reconstructs the split by:

1. Extracting the prefix and postfix tuple data from the WAL record
2. Creating proper tuple headers for both tuples (handling unaligned data)
3. Inserting the postfix tuple first (to avoid dangling links)
4. Updating the original page with the new prefix tuple
5. Handling cases where tuples are on the same page or different pages

The function ensures consistency during recovery by processing pages in the correct order and properly managing buffer locks and LSN updates.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record data with split tuple information

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogInitBufferForRedo
  - XLogReadBufferForRedo
  - [SpGistInitBuffer](../S/SpGistInitBuffer.md)
  - [addOrReplaceTuple](../a/addOrReplaceTuple.md)
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md)
  - PageAddItem
  - [PageSetLSN](../P/PageSetLSN.md)
  - MarkBufferDirty
  - [BufferGetPage](../B/BufferGetPage.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Called from:
  - [spg_redo](spg_redo.md) (main SP-GiST redo dispatcher)

## Notes and Other Information
- Handles unaligned tuple data by copying headers to aligned structures
- Processes postfix tuple insertion before prefix tuple to maintain referential integrity
- Supports both same-page and cross-page tuple splits
- Uses proper WAL recovery ordering to ensure consistency
- Part of the SP-GiST index WAL recovery subsystem located in src/backend/access/spgist/spgxlog.c:451-528