# _bt_unlink_halfdead_page

## Location
[src/backend/access/nbtree/nbtpage.c:2314-2812](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L2314-L2812)

## Overview
This function performs the second stage of B-tree page deletion by unlinking a single page from its siblings and marking it as fully deleted, progressing through the subtree from top parent to leaf.

## Definition

```c
static bool
_bt_unlink_halfdead_page(Relation rel, Buffer leafbuf, BlockNumber scanblkno,
						 bool *rightsib_empty, BTVacState *vstate)
```
## Detailed Description
_bt_unlink_halfdead_page implements the unlinking phase of B-tree page deletion, removing one page at a time from the half-dead subtree established by _bt_mark_page_halfdead. The function operates iteratively - each call removes the current top parent of the subtree, with the leaf page being deleted in the final call.

The function performs several key operations:
1. Determines the target page to unlink (either leaf page or current top parent from leaf's high key)
2. Acquires locks on target page and its siblings in proper order (left-to-right, then up) to avoid deadlocks
3. Validates sibling links to detect index corruption and recover gracefully
4. Updates sibling pointers to bypass the target page
5. Updates the leaf page's top parent reference if an internal page was deleted
6. Marks the target page as fully deleted with a safe transaction ID
7. Updates metapage fast root if the deletion affects the tree structure

The function includes extensive corruption detection and recovery mechanisms, logging warnings rather than failing completely when sibling link inconsistencies are detected.

## Parameters
- : The B-tree index relation being modified
- : Buffer containing the original leaf page (maintained throughout subtree deletion)
- : Block number from the original VACUUM scan (used for statistics)
- : Output parameter indicating if the right sibling is empty
- : VACUUM state containing statistics and FSM tracking information

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (gets page block numbers)
  - BTPageGetOpaque (accesses B-tree page metadata)
  - [BTreeTupleGetTopParent](../B/BTreeTupleGetTopParent.md), BTreeTupleSetTopParent (manages top parent references)
  - [_bt_getbuf](_bt_getbuf.md), _bt_relbuf, _bt_lockbuf, _bt_unlockbuf (buffer management)
  - P_ISLEAF, P_ISDELETED, P_ISHALFDEAD, P_RIGHTMOST (page state checks)
  - [BTPageSetDeleted](../B/BTPageSetDeleted.md) (marks page as deleted with transaction ID)
  - [ReadNextFullTransactionId](../R/ReadNextFullTransactionId.md) (gets safe deletion timestamp)
  - [_bt_upgrademetapage](_bt_upgrademetapage.md) (upgrades metapage format if needed)
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterBuffer, XLogRegisterData, XLogInsert (WAL logging)
  - [_bt_pendingfsm_add](_bt_pendingfsm_add.md) (tracks page for FSM inclusion)
- Called from:
  - [_bt_pagedel](_bt_pagedel.md) (calls iteratively until entire subtree is deleted)

## Notes and Other Information
- Returns false on failure (should not happen under normal circumstances)
- Returns true on success, with leafbuf remaining locked for caller
- Handles both leaf page deletion and internal page deletion in the same function
- Updates VACUUM statistics (pages_newly_deleted and pages_deleted) appropriately
- Implements robust error handling for sibling link corruption, continuing VACUUM rather than failing
- May update metapage fast root when deleting causes a level to become empty
- Uses proper locking order (left-to-right, then up) to prevent deadlocks
- Maintains referential integrity by updating all sibling pointers before marking page deleted
- The deleted page becomes a tombstone that can be recycled when all concurrent transactions complete