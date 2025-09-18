# BTPageSetDeleted

## Location
src/include/access/nbtree.h: 239 - 259

## Overview
BTPageSetDeleted is a static inline function that marks a B-tree page as deleted and sets the safe transaction ID for when the page can be recycled.

## Definition
```c
static inline void
BTPageSetDeleted(Page page, FullTransactionId safexid)
```

## Detailed Description
This function performs the complete marking of a B-tree page as deleted. It updates the page's opaque area flags to indicate the page is deleted and contains a full transaction ID, adjusts the page's lower and upper pointers to make room for the deletion metadata, and stores the safe transaction ID that indicates when this page can be safely recycled.

The function clears the BTP_HALF_DEAD flag (if set) and sets both BTP_DELETED and BTP_HAS_FULLXID flags. It also adjusts the page layout to store the BTDeletedPageData structure containing the safexid.

## Parameters / Member Variables
- `page`: The B-tree page to be marked as deleted
- `safexid`: The full transaction ID that indicates when this page can be safely recycled (when all transactions older than this ID have completed)

## Dependencies
- Functions called/Symbols referenced:
  - BTPageGetOpaque
  - [PageGetContents](../P/PageGetContents.md)
  - BTPageOpaque (type)
  - PageHeader (type)
  - [BTDeletedPageData](BTDeletedPageData.md) (type)
  - FullTransactionId (type)
  - BTP_HALF_DEAD, BTP_DELETED, BTP_HAS_FULLXID (flags)
  - SizeOfPageHeaderData (constant)
- Called from (representative examples):
  - [_bt_unlink_halfdead_page](../b/_bt_unlink_halfdead_page.md)
  - [btree_xlog_unlink_page](../b/btree_xlog_unlink_page.md)

## Notes and Other Information
This function is typically called during B-tree page deletion operations when a page is being unlinked from the tree structure. The safexid parameter is crucial for MVCC (Multi-Version Concurrency Control) as it ensures the page isn't recycled while older transactions might still need to access it. The function is defined as a static inline in the header file for performance reasons since it's a frequently used operation in B-tree maintenance.