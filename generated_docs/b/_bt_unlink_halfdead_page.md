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

## Simplified Source

```c
static bool _bt_unlink_halfdead_page(Relation rel, Buffer leafbuf, BlockNumber scanblkno,
                                     bool *rightsib_empty, BTVacState *vstate)
{
    Page page = BufferGetPage(leafbuf);
    BTPageOpaque opaque = BTPageGetOpaque(page);
    IndexBulkDeleteResult *stats = vstate->stats;

    // Get target page info from leaf's high key
    ItemId itemid = PageGetItemId(page, P_HIKEY);
    IndexTuple leafhikey = (IndexTuple) PageGetItem(page, itemid);
    BlockNumber target = BTreeTupleGetTopParent(leafhikey);
    BlockNumber leafleftsib = opaque->btpo_prev;
    BlockNumber leafrightsib = opaque->btpo_next;

    _bt_unlockbuf(rel, leafbuf);
    CHECK_FOR_INTERRUPTS();

    // Determine target page to unlink
    Buffer buf, lbuf = InvalidBuffer, rbuf;
    BlockNumber leftsib;
    uint32 targetlevel;

    if (!BlockNumberIsValid(target)) {
        // Target is leaf page itself
        target = BufferGetBlockNumber(leafbuf);
        buf = leafbuf;
        leftsib = leafleftsib;
        targetlevel = 0;
    } else {
        // Target is internal page
        buf = _bt_getbuf(rel, target, BT_READ);
        page = BufferGetPage(buf);
        opaque = BTPageGetOpaque(page);
        leftsib = opaque->btpo_prev;
        targetlevel = opaque->btpo_level;
        _bt_unlockbuf(rel, buf);
    }

    // Lock pages in proper order: left sibling, target, right sibling
    if (target != BufferGetBlockNumber(leafbuf))
        _bt_lockbuf(rel, leafbuf, BT_WRITE);

    if (leftsib != P_NONE) {
        lbuf = _bt_getbuf(rel, leftsib, BT_WRITE);
        // Handle case where left sibling was split/moved
        page = BufferGetPage(lbuf);
        opaque = BTPageGetOpaque(page);
        while (P_ISDELETED(opaque) || opaque->btpo_next != target) {
            leftsib = opaque->btpo_next;
            _bt_relbuf(rel, lbuf);
            if (P_RIGHTMOST(opaque) || P_ISDELETED(opaque)) {
                // Corruption detected - abort
                ReleaseBuffer(buf);
                if (target != BufferGetBlockNumber(leafbuf))
                    _bt_relbuf(rel, leafbuf);
                return false;
            }
            lbuf = _bt_getbuf(rel, leftsib, BT_WRITE);
            page = BufferGetPage(lbuf);
            opaque = BTPageGetOpaque(page);
        }
    }

    _bt_lockbuf(rel, buf, BT_WRITE);
    page = BufferGetPage(buf);
    opaque = BTPageGetOpaque(page);

    // Final safety checks
    if (P_RIGHTMOST(opaque) || P_ISROOT(opaque) || P_ISDELETED(opaque))
        elog(ERROR, "target page changed status unexpectedly");

    BlockNumber rightsib = opaque->btpo_next;
    rbuf = _bt_getbuf(rel, rightsib, BT_WRITE);

    // Check if right sibling is empty for caller
    page = BufferGetPage(rbuf);
    opaque = BTPageGetOpaque(page);
    *rightsib_empty = (P_FIRSTDATAKEY(opaque) > PageGetMaxOffsetNumber(page));

    START_CRIT_SECTION();

    // Update sibling links to bypass target page
    if (BufferIsValid(lbuf)) {
        page = BufferGetPage(lbuf);
        opaque = BTPageGetOpaque(page);
        opaque->btpo_next = rightsib;
    }
    page = BufferGetPage(rbuf);
    opaque = BTPageGetOpaque(page);
    opaque->btpo_prev = leftsib;

    // Update leaf's top parent if we deleted an internal page
    BlockNumber leaftopparent = InvalidBlockNumber;
    if (target != BufferGetBlockNumber(leafbuf)) {
        // Set up next iteration
        page = BufferGetPage(buf);
        opaque = BTPageGetOpaque(page);
        if (P_FIRSTDATAKEY(opaque) <= PageGetMaxOffsetNumber(page)) {
            itemid = PageGetItemId(page, P_FIRSTDATAKEY(opaque));
            IndexTuple finaldataitem = (IndexTuple) PageGetItem(page, itemid);
            leaftopparent = BTreeTupleGetDownLink(finaldataitem);
            if (leaftopparent == BufferGetBlockNumber(leafbuf))
                leaftopparent = InvalidBlockNumber;
        }
        BTreeTupleSetTopParent(leafhikey, leaftopparent);
    }

    // Mark target page as deleted
    page = BufferGetPage(buf);
    FullTransactionId safexid = ReadNextFullTransactionId();
    BTPageSetDeleted(page, safexid);

    // Mark all buffers dirty and perform WAL logging
    MarkBufferDirty(rbuf);
    MarkBufferDirty(buf);
    if (BufferIsValid(lbuf))
        MarkBufferDirty(lbuf);
    if (target != BufferGetBlockNumber(leafbuf))
        MarkBufferDirty(leafbuf);

    if (RelationNeedsWAL(rel)) {
        // WAL logging code...
    }

    END_CRIT_SECTION();

    // Release locks and update statistics
    if (BufferIsValid(lbuf))
        _bt_relbuf(rel, lbuf);
    _bt_relbuf(rel, rbuf);
    if (target != BufferGetBlockNumber(leafbuf))
        _bt_relbuf(rel, buf);

    stats->pages_newly_deleted++;
    if (target <= scanblkno)
        stats->pages_deleted++;

    _bt_pendingfsm_add(vstate, target, safexid);
    return true;
}
```