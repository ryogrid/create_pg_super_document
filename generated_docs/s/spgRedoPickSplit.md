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

## Simplified Source

```c
static void spgRedoPickSplit(XLogReaderState *record) {
    // Extract WAL record data and setup
    spgxlogPickSplit *xldata = (spgxlogPickSplit *) XLogRecGetData(record);
    SpGistState state;
    fillFakeState(&state, xldata->stateSrc);

    // Parse arrays from record data
    OffsetNumber *toDelete = /* extracted from record data */;
    OffsetNumber *toInsert = /* extracted from record data */;
    uint8 *leafPageSelect = /* extracted from record data */;
    char *innerTuple = /* extracted from record data */;
    SpGistInnerTupleData innerTupleHdr;
    memcpy(&innerTupleHdr, innerTuple, sizeof(SpGistInnerTupleData));

    BlockNumber blknoInner;
    XLogRecGetBlockTag(record, 2, NULL, NULL, &blknoInner);

    // Handle source page
    Buffer srcBuffer = InvalidBuffer;
    Page srcPage = NULL;
    if (xldata->isRootSplit) {
        // Root split - no source page processing needed
    } else if (xldata->initSrc) {
        // Re-initialize source page
        srcBuffer = XLogInitBufferForRedo(record, 0);
        srcPage = BufferGetPage(srcBuffer);
        SpGistInitBuffer(srcBuffer, SPGIST_LEAF | (xldata->storesNulls ? SPGIST_NULLS : 0));
    } else {
        // Delete tuples from source page and add redirections
        if (XLogReadBufferForRedo(record, 0, &srcBuffer) == BLK_NEEDS_REDO) {
            srcPage = BufferGetPage(srcBuffer);

            // Delete old tuples, add redirections or placeholders
            if (!state.isBuild) {
                spgPageIndexMultiDelete(&state, srcPage, toDelete, xldata->nDelete,
                                       SPGIST_REDIRECT, SPGIST_PLACEHOLDER,
                                       blknoInner, xldata->offnumInner);
            } else {
                spgPageIndexMultiDelete(&state, srcPage, toDelete, xldata->nDelete,
                                       SPGIST_PLACEHOLDER, SPGIST_PLACEHOLDER,
                                       InvalidBlockNumber, InvalidOffsetNumber);
            }
        }
    }

    // Handle destination page (if exists)
    Buffer destBuffer = InvalidBuffer;
    Page destPage = NULL;
    if (XLogRecHasBlockRef(record, 1)) {
        if (xldata->initDest) {
            // Initialize destination page
            destBuffer = XLogInitBufferForRedo(record, 1);
            destPage = BufferGetPage(destBuffer);
            SpGistInitBuffer(destBuffer, SPGIST_LEAF | (xldata->storesNulls ? SPGIST_NULLS : 0));
        } else {
            // Use existing destination page
            if (XLogReadBufferForRedo(record, 1, &destBuffer) == BLK_NEEDS_REDO) {
                destPage = BufferGetPage(destBuffer);
            }
        }
    }

    // Restore leaf tuples to appropriate pages
    char *leafTuplePtr = /* points to leaf tuple data */;
    for (int i = 0; i < xldata->nInsert; i++) {
        SpGistLeafTupleData leafTupleHdr;
        memcpy(&leafTupleHdr, leafTuplePtr, sizeof(SpGistLeafTupleData));

        // Select target page based on leafPageSelect array
        Page targetPage = leafPageSelect[i] ? destPage : srcPage;
        if (targetPage != NULL) {
            addOrReplaceTuple(targetPage, leafTuplePtr, leafTupleHdr.size, toInsert[i]);
        }
        leafTuplePtr += leafTupleHdr.size;
    }

    // Update source and destination page LSNs
    if (srcPage != NULL) {
        PageSetLSN(srcPage, record->EndRecPtr);
        MarkBufferDirty(srcBuffer);
    }
    if (destPage != NULL) {
        PageSetLSN(destPage, record->EndRecPtr);
        MarkBufferDirty(destBuffer);
    }

    // Restore new inner tuple
    Buffer innerBuffer;
    if (xldata->initInner) {
        innerBuffer = XLogInitBufferForRedo(record, 2);
        SpGistInitBuffer(innerBuffer, (xldata->storesNulls ? SPGIST_NULLS : 0));
    } else {
        XLogReadBufferForRedo(record, 2, &innerBuffer);
    }

    Page innerPage = BufferGetPage(innerBuffer);
    addOrReplaceTuple(innerPage, innerTuple, innerTupleHdr.size, xldata->offnumInner);

    // Update parent link if inner is also parent
    if (xldata->innerIsParent) {
        SpGistInnerTuple parentTuple = (SpGistInnerTuple) PageGetItem(innerPage,
                                       PageGetItemId(innerPage, xldata->offnumParent));
        spgUpdateNodeLink(parentTuple, xldata->nodeI, blknoInner, xldata->offnumInner);
    }

    PageSetLSN(innerPage, record->EndRecPtr);
    MarkBufferDirty(innerBuffer);
    UnlockReleaseBuffer(innerBuffer);

    // Release leaf page buffers
    if (BufferIsValid(srcBuffer)) UnlockReleaseBuffer(srcBuffer);
    if (BufferIsValid(destBuffer)) UnlockReleaseBuffer(destBuffer);

    // Update parent downlink if on separate page
    if (XLogRecHasBlockRef(record, 3)) {
        Buffer parentBuffer;
        if (XLogReadBufferForRedo(record, 3, &parentBuffer) == BLK_NEEDS_REDO) {
            Page parentPage = BufferGetPage(parentBuffer);
            SpGistInnerTuple parentTuple = (SpGistInnerTuple) PageGetItem(parentPage,
                                           PageGetItemId(parentPage, xldata->offnumParent));
            spgUpdateNodeLink(parentTuple, xldata->nodeI, blknoInner, xldata->offnumInner);

            PageSetLSN(parentPage, record->EndRecPtr);
            MarkBufferDirty(parentBuffer);
        }
        UnlockReleaseBuffer(parentBuffer);
    }
}
```