# btree_xlog_split

## Location
src/backend/access/nbtree/nbtxlog.c: 251 - 463

## Overview
Handles B-tree page split WAL record replay during recovery, reconstructing both left and right pages from the split operation.

## Definition
```c
static void btree_xlog_split(bool newitemonleft, XLogReaderState *record)
```

## Detailed Description
This function is one of the most complex WAL replay operations in the B-tree implementation. It reconstructs a complete B-tree page split operation from WAL record data. A page split occurs when a B-tree page becomes too full and needs to be divided into two pages: the original (left) page and a new (right) page.

The function handles multiple complex scenarios:
1. **Basic page split**: Dividing tuples between left and right pages
2. **Posting list splits**: When the split involves compressed posting lists
3. **Chain link management**: Properly updating prev/next pointers between pages
4. **Incomplete split handling**: Managing the BTP_INCOMPLETE_SPLIT flag
5. **New item insertion**: Inserting the triggering item during split replay

The process involves reconstructing the right page from scratch using _bt_restore_page, then carefully reconstructing the left page by creating a temporary page and adding items in the correct order to maintain physical tuple ordering for WAL consistency checking.

For internal page splits, it also clears incomplete split flags on child pages, maintaining tree consistency across levels.

## Parameters / Member Variables
- `newitemonleft`: Boolean indicating whether the new item that triggered the split should be placed on the left page
- `record`: XLogReaderState containing the complete WAL record data for the split operation

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_clear_incomplete_split](_bt_clear_incomplete_split.md)
  - XLogRecGetData
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md)
  - [XLogRecGetBlockTagExtended](../X/XLogRecGetBlockTagExtended.md)
  - XLogInitBufferForRedo
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - XLogReadBufferForRedo
  - [_bt_pageinit](_bt_pageinit.md)
  - [_bt_restore_page](_bt_restore_page.md)
  - BTPageGetOpaque
  - [PageGetTempPageCopySpecial](../P/PageGetTempPageCopySpecial.md)
  - PageAddItem
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - ItemIdGetLength
  - OffsetNumberPrev
  - OffsetNumberNext
  - [CopyIndexTuple](../C/CopyIndexTuple.md)
  - [_bt_swap_posting](_bt_swap_posting.md)
  - [PageRestoreTempPage](../P/PageRestoreTempPage.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - MarkBufferDirty
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Data types used:
  - [xl_btree_split](../x/xl_btree_split.md)
  - BTPageOpaque
  - ItemId
  - [IndexTuple](../I/IndexTuple.md)
- Constants used:
  - BLK_NEEDS_REDO
  - P_NONE
  - BTP_LEAF
  - BTP_INCOMPLETE_SPLIT
  - P_HIKEY
  - P_FIRSTDATAKEY
  - InvalidOffsetNumber
- Called from (representative examples):
  - [btree_redo](btree_redo.md) (for both leaf and internal page splits)

## Notes and Other Information
- This is a static function used internally within nbtxlog.c for B-tree WAL recovery
- One of the most complex WAL replay operations due to the intricate nature of B-tree splits
- Handles both leaf and internal page splits with appropriate flag management
- Supports posting list splits for compressed leaf page entries
- Maintains physical tuple ordering for WAL consistency checking by using temporary pages
- Carefully manages buffer release order to prevent readers from observing inconsistent states
- Sets BTP_INCOMPLETE_SPLIT flag on the left page until the parent gets the downlink
- Updates sibling page links (prev/next pointers) to maintain B-tree chain integrity
- Includes comprehensive error handling with detailed error messages for debugging
- The function mirrors the logic of the original _bt_split() function during replay
- Critical for maintaining B-tree consistency during crash recovery scenarios