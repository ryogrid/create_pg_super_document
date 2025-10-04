# spgRedoSplitTuple

## Location
[src/backend/access/spgist/spgxlog.c:451-528](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgxlog.c#L451-L528)

## Overview
Replays a split tuple operation from the WAL (Write-Ahead Log) for SP-GiST indexes, handling the reconstruction of prefix and postfix tuples during crash recovery.

## Definition

```c
static void
spgRedoSplitTuple(XLogReaderState *record)
```
## Detailed Description
This function is part of the SP-GiST WAL recovery mechanism that replays split tuple operations. When an SP-GiST inner tuple is split during normal operation, this operation is logged to WAL. During recovery, this function reconstructs the split by:

1. Extracting the prefix and postfix tuple data from the WAL record
2. Creating proper tuple headers for both tuples (handling unaligned data)
3. Inserting the postfix tuple first (to avoid dangling links)
4. Updating the original page with the new prefix tuple
5. Handling cases where tuples are on the same page or different pages

The function ensures consistency during recovery by processing pages in the correct order and properly managing buffer locks and LSN updates.

## Parameters / Member Variables
- `*record`: XLogReaderState pointer containing the WAL record data with split tuple information
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - [SpGistInitBuffer](../S/SpGistInitBuffer.md)
  - [addOrReplaceTuple](../a/addOrReplaceTuple.md)
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md)
  - PageAddItem
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
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

## Simplified Source

```c
static void spgRedoSplitTuple(XLogReaderState *record) {
    // Extract WAL record data and setup
    spgxlogSplitTuple *xldata = (spgxlogSplitTuple *) XLogRecGetData(record);

    // Extract prefix and postfix tuples from record data
    char *prefixTuple = /* extracted from record data */;
    SpGistInnerTupleData prefixTupleHdr;
    memcpy(&prefixTupleHdr, prefixTuple, sizeof(SpGistInnerTupleData));

    char *postfixTuple = /* extracted after prefix tuple */;
    SpGistInnerTupleData postfixTupleHdr;
    memcpy(&postfixTupleHdr, postfixTuple, sizeof(SpGistInnerTupleData));

    // Step 1: Insert postfix tuple first (to avoid dangling links)
    if (!xldata->postfixBlkSame) {
        // Postfix goes to separate page
        Buffer postfixBuffer;
        if (xldata->newPage) {
            postfixBuffer = XLogInitBufferForRedo(record, 1);
            SpGistInitBuffer(postfixBuffer, 0);  // SplitTuple not used for nulls pages
        } else {
            XLogReadBufferForRedo(record, 1, &postfixBuffer);
        }

        Page postfixPage = BufferGetPage(postfixBuffer);
        addOrReplaceTuple(postfixPage, postfixTuple, postfixTupleHdr.size, xldata->offnumPostfix);

        PageSetLSN(postfixPage, record->EndRecPtr);
        MarkBufferDirty(postfixBuffer);
        UnlockReleaseBuffer(postfixBuffer);
    }

    // Step 2: Handle original page - replace prefix tuple and add postfix if same page
    Buffer prefixBuffer;
    if (XLogReadBufferForRedo(record, 0, &prefixBuffer) == BLK_NEEDS_REDO) {
        Page prefixPage = BufferGetPage(prefixBuffer);

        // Replace old tuple with new prefix tuple
        PageIndexTupleDelete(prefixPage, xldata->offnumPrefix);
        PageAddItem(prefixPage, prefixTuple, prefixTupleHdr.size, xldata->offnumPrefix, false, false);

        // If postfix is on same page, add it here
        if (xldata->postfixBlkSame) {
            addOrReplaceTuple(prefixPage, postfixTuple, postfixTupleHdr.size, xldata->offnumPostfix);
        }

        PageSetLSN(prefixPage, record->EndRecPtr);
        MarkBufferDirty(prefixBuffer);
    }
    UnlockReleaseBuffer(prefixBuffer);
}
```