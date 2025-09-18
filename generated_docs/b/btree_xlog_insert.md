# btree_xlog_insert

## Location
src/backend/access/nbtree/nbtxlog.c: 160 - 250

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
  - XLogReadBufferForRedo
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - PageAddItem
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - OffsetNumberPrev
  - [CopyIndexTuple](../C/CopyIndexTuple.md)
  - [_bt_swap_posting](_bt_swap_posting.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - MarkBufferDirty
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