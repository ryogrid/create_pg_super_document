# btree_xlog_insert

## Location
[src/backend/access/nbtree/nbtxlog.c:160-250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtxlog.c#L160-L250)

## Overview
Handles B-tree insertion WAL record replay during recovery, supporting both simple insertions and posting list splits.

## Definition
```c
static void btree_xlog_insert(bool isleaf, bool ismeta, bool posting, XLogReaderState *record)
```

## Detailed Description
This function replays B-tree insertion operations during WAL recovery. It handles two types of insertions: simple retail insertions and more complex posting list splits that occur when inserting into compressed posting lists on leaf pages.

For internal page insertions (non-leaf), the function first clears the incomplete split flag from the child page, completing a previously interrupted split operation. This maintains B-tree consistency by ensuring that downlink insertions properly complete their associated splits.

The function supports posting list splits, which are optimizations used in B-tree leaf pages to compress multiple tuples with identical keys. When a posting list becomes too large, it's split into multiple entries, and this function handles the replay of such splits by using the _bt_swap_posting mechanism to reconstruct the split operation.

If the insertion involves metadata changes (ismeta=true), the function also updates the B-tree metapage to maintain index consistency.

## Parameters / Member Variables
- `isleaf`: Boolean indicating whether the insertion is on a leaf page
- `ismeta`: Boolean indicating whether metapage update is required
- `posting`: Boolean indicating whether this is a posting list split operation
- `record`: XLogReaderState containing the WAL record data for the insertion

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_clear_incomplete_split](_bt_clear_incomplete_split.md)
  - XLogRecGetData
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - PageAddItem
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - OffsetNumberPrev
  - [CopyIndexTuple](../C/CopyIndexTuple.md)
  - [_bt_swap_posting](_bt_swap_posting.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [_bt_restore_meta](_bt_restore_meta.md)
- Data types used:
  - [xl_btree_insert](../x/xl_btree_insert.md)
  - ItemId
  - [IndexTuple](../I/IndexTuple.md)
- Constants used:
  - BLK_NEEDS_REDO
  - InvalidOffsetNumber
- Called from (representative examples):
  - [btree_redo](btree_redo.md) (multiple call sites for different insertion types)

## Notes and Other Information
- This is a static function used internally within nbtxlog.c for B-tree WAL recovery
- Handles both simple insertions and complex posting list split scenarios
- For non-leaf insertions, always clears incomplete split flags to maintain consistency
- Uses different logic paths for regular insertions vs. posting list splits
- During posting list splits, processes posting offset and reconstructs the split using _bt_swap_posting
- Metapage updates are performed last to maintain consistency during recovery
- Includes panic-level error handling for critical insertion failures
- Optimized for replay scenarios where concurrent access isn't a concern
- The function ensures atomic completion of operations that may have been interrupted during the original crash

## Simplified Source

```c
static void btree_xlog_insert(bool isleaf, bool ismeta, bool posting,
                             XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_btree_insert *xlrec = (xl_btree_insert *) XLogRecGetData(record);
    Buffer buffer;
    Page page;

    // For internal pages, clear incomplete split flag from child
    if (!isleaf)
        _bt_clear_incomplete_split(record, 1);

    // Process the main insertion if buffer needs redo
    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
    {
        Size datalen;
        char *datapos = XLogRecGetBlockData(record, 0, &datalen);
        page = BufferGetPage(buffer);

        if (!posting)
        {
            // Simple insertion: add the new item to the page
            if (PageAddItem(page, (Item) datapos, datalen, xlrec->offnum,
                           false, false) == InvalidOffsetNumber)
                elog(PANIC, "failed to add new item");
        }
        else
        {
            // Posting list split: more complex handling
            ItemId itemid;
            IndexTuple oposting, newitem, nposting;
            uint16 postingoff;

            // Extract posting split offset from WAL data
            postingoff = *((uint16 *) datapos);
            datapos += sizeof(uint16);
            datalen -= sizeof(uint16);

            // Get the existing posting list that was split
            itemid = PageGetItemId(page, OffsetNumberPrev(xlrec->offnum));
            oposting = (IndexTuple) PageGetItem(page, itemid);

            // Recreate the posting list split
            newitem = CopyIndexTuple((IndexTuple) datapos);
            nposting = _bt_swap_posting(newitem, oposting, postingoff);

            // Replace old posting list with split version
            memcpy(oposting, nposting, MAXALIGN(IndexTupleSize(nposting)));

            // Insert the final new item
            if (PageAddItem(page, (Item) newitem, datalen, xlrec->offnum,
                           false, false) == InvalidOffsetNumber)
                elog(PANIC, "failed to add posting split new item");
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    if (BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);

    // Update metapage if required
    if (ismeta)
        _bt_restore_meta(record, 2);
}
```