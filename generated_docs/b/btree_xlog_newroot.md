# btree_xlog_newroot

## Location
[src/backend/access/nbtree/nbtxlog.c:937-1002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtxlog.c#L937-L1002)

## Overview
Replays the creation of a new B-tree root page during WAL recovery, establishing a new root level for the index.

## Definition
```c
static void btree_xlog_newroot(XLogReaderState *record)
```

## Detailed Description
This function handles the replay of B-tree root page creation operations during Write-Ahead Log (WAL) recovery. When a B-tree grows in height due to a root split, a new root page must be created at a higher level. This function reconstructs the new root page from the WAL record data.

The function performs several key operations:
1. Initializes a new page as the root page with appropriate opaque data
2. Sets the root page flags (BTP_ROOT) and leaf flags if applicable (level 0)
3. For non-leaf roots (level > 0), restores the page content from WAL record data
4. Clears any incomplete-split flags in the left child page
5. Updates the metapage to reflect the new root page and tree height

The new root page has no siblings (prev/next pointers set to P_NONE) and is marked with the BTP_ROOT flag to identify it as the root of the B-tree.

## Parameters / Member Variables
- `record`: XLogReaderState containing the WAL record data with root page content and block references

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - BTPageGetOpaque
  - [_bt_pageinit](_bt_pageinit.md)
  - [_bt_restore_page](_bt_restore_page.md)
  - [_bt_clear_incomplete_split](_bt_clear_incomplete_split.md)
  - [_bt_restore_meta](_bt_restore_meta.md)
  - [BufferGetPageSize](../B/BufferGetPageSize.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Called from (representative examples):
  - [btree_redo](btree_redo.md)

## Notes and Other Information
- The new root page is always initialized with prev/next pointers set to P_NONE
- For leaf-level roots (level 0), the BTP_LEAF flag is set in addition to BTP_ROOT
- Non-leaf roots have their content restored from block data in the WAL record
- Block references in WAL record: [0] new root page, [1] left child (for incomplete split clearing), [2] metapage
- The metapage is always updated to reflect the new root page location and increased tree height
- This operation typically occurs during B-tree growth when the original root needs to be split

## Simplified Source

```c
static void
btree_xlog_newroot(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_btree_newroot *xlrec = (xl_btree_newroot *) XLogRecGetData(record);
    Buffer buffer;
    Page page;
    BTPageOpaque pageop;

    // Initialize new root page
    buffer = XLogInitBufferForRedo(record, 0);
    page = (Page) BufferGetPage(buffer);

    _bt_pageinit(page, BufferGetPageSize(buffer));
    pageop = BTPageGetOpaque(page);

    // Set root page metadata
    pageop->btpo_flags = BTP_ROOT;
    pageop->btpo_prev = pageop->btpo_next = P_NONE;
    pageop->btpo_level = xlrec->level;
    if (xlrec->level == 0)
        pageop->btpo_flags |= BTP_LEAF;
    pageop->btpo_cycleid = 0;

    // For non-leaf roots, restore content and clear incomplete split
    if (xlrec->level > 0)
    {
        char *ptr;
        Size len;

        ptr = XLogRecGetBlockData(record, 0, &len);
        _bt_restore_page(page, ptr, len);
        _bt_clear_incomplete_split(record, 1);
    }

    PageSetLSN(page, lsn);
    MarkBufferDirty(buffer);
    UnlockReleaseBuffer(buffer);

    // Update metapage with new root
    _bt_restore_meta(record, 2);
}
```