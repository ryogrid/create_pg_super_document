# btree_xlog_newroot

## Location
src/backend/access/nbtree/nbtxlog.c: 937 - 1002

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
  - XLogInitBufferForRedo
  - XLogRecGetBlockData
  - BTPageGetOpaque
  - _bt_pageinit
  - _bt_restore_page
  - _bt_clear_incomplete_split
  - _bt_restore_meta
  - BufferGetPageSize
  - PageSetLSN
  - MarkBufferDirty
  - UnlockReleaseBuffer
- Called from (representative examples):
  - btree_redo

## Notes and Other Information
- The new root page is always initialized with prev/next pointers set to P_NONE
- For leaf-level roots (level 0), the BTP_LEAF flag is set in addition to BTP_ROOT
- Non-leaf roots have their content restored from block data in the WAL record
- Block references in WAL record: [0] new root page, [1] left child (for incomplete split clearing), [2] metapage
- The metapage is always updated to reflect the new root page location and increased tree height
- This operation typically occurs during B-tree growth when the original root needs to be split