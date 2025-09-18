# _bt_mark_page_halfdead

## Location
src/backend/access/nbtree/nbtpage.c: 2088 - 2313

## Overview
This function performs the first stage of B-tree page deletion by determining the subtree to be deleted, removing downlinks from the parent page, and marking the leaf page as half-dead.

## Definition


## Detailed Description
_bt_mark_page_halfdead implements the initial phase of B-tree page deletion, establishing the height of the subtree that needs to be deleted and preparing it for the unlinking phase. The function determines whether deletion can safely proceed by checking for rightmost child constraints and half-dead right siblings.

The function performs several critical operations:
1. Validates that the right sibling is not half-dead (which would complicate parent page operations)
2. Uses _bt_lock_subtree_parent to determine the full subtree that must be deleted and lock the subtree parent
3. Updates the parent page by overwriting the downlink to point to the right sibling and removing the following pivot tuple
4. Marks the leaf page with BTP_HALF_DEAD flag and stores a link to the top parent page in the high key
5. Logs the operation with a WAL record for crash recovery

The function implements a key space movement strategy where deleted key ranges are absorbed by the right sibling, following the approach described in the nbtree/README.

## Parameters
- : The B-tree index relation being modified
- : The heap relation (required for potential page allocation during parent locking)
- : Buffer containing the target leaf page to mark as half-dead
- : Search stack pointing to the approximate parent page location

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_rightsib_halfdeadflag](_bt_rightsib_halfdeadflag.md) (checks if right sibling is safe for deletion)
  - [_bt_lock_subtree_parent](_bt_lock_subtree_parent.md) (determines subtree height and locks parent)
  - [BufferGetPage](../B/BufferGetPage.md), BTPageGetOpaque (page access functions)
  - [PageGetItemId](../P/PageGetItemId.md), PageGetItem (tuple access functions)
  - [BTreeTupleGetDownLink](../B/BTreeTupleGetDownLink.md), BTreeTupleSetDownLink (downlink manipulation)
  - [BTreeTupleSetTopParent](../B/BTreeTupleSetTopParent.md) (stores top parent reference in leaf page)
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md), PageIndexTupleOverwrite (page modification functions)
  - [PredicateLockPageCombine](../P/PredicateLockPageCombine.md) (serializable isolation support)
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterBuffer, XLogRegisterData, XLogInsert (WAL logging)
- Called from:
  - [_bt_pagedel](_bt_pagedel.md) (main page deletion coordinator)

## Notes and Other Information
- Returns false if deletion cannot proceed safely (rightmost child or half-dead right sibling)
- Returns true when the first phase completes successfully
- Uses critical sections to ensure atomic updates with proper WAL logging
- Stores the top parent block number in the leaf page's high key for the unlinking phase
- Implements key space movement to the right (opposite of Lanin and Shasha algorithm)
- Includes extensive corruption detection and logging for debugging index issues
- The marked half-dead pages will be fully unlinked by subsequent calls to _bt_unlink_halfdead_page