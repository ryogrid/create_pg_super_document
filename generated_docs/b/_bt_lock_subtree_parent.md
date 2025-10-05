# _bt_lock_subtree_parent

## Location
[src/backend/access/nbtree/nbtpage.c:2813-2953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L2813-L2953)

## Overview
This recursive function determines the height of the subtree that can be safely deleted and locks the parent of the subtree root, establishing the boundaries for safe page deletion in B-tree indexes.

## Definition

```c
static bool
_bt_lock_subtree_parent(Relation rel, Relation heaprel, BlockNumber child,
						BTStack stack, Buffer *subtreeparent,
						OffsetNumber *poffset, BlockNumber *topparent,
						BlockNumber *topparentrightsib)
```
## Detailed Description
_bt_lock_subtree_parent is a recursive function that implements the core logic for determining whether B-tree page deletion is safe by analyzing the relationship between pages and their parents. The function starts from a target child page and works its way up the tree, checking whether each level can be safely deleted according to B-tree deletion rules.

The key principle is that a page can only be deleted if it's not the rightmost child of its parent, OR if the parent can also be deleted (in which case the entire subtree is removed). The function recursively applies this rule up the tree until it finds a safe deletion boundary.

Key operations include:
1. Using _bt_getstackbuf to locate and lock the parent page containing the downlink to the child
2. Checking if the child is the rightmost child of its parent
3. If not rightmost, the deletion is safe and the function returns successfully
4. If rightmost, checking if the parent can also be deleted (parent must have only one child and not be rightmost itself)
5. Recursively calling itself with the parent as the new child to check the next level up
6. Updating the topparent and topparentrightsib references to reflect the actual root of the deletable subtree

## Parameters
- : The B-tree index relation being processed
- : The heap relation (needed for potential page allocation during stack operations)
- : Block number of the child page being evaluated for deletion
- : Search stack leading to the child page (updated during processing)
- : Output parameter - buffer for the parent of the subtree root (locked for caller)
- : Output parameter - offset of the pivot tuple containing the downlink to be removed
- : Input/output parameter - block number of the subtree root (updated as recursion proceeds)
- : Input/output parameter - right sibling of the subtree root (updated as recursion proceeds)

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_getstackbuf](_bt_getstackbuf.md) (locates and locks parent page containing downlink to child)
  - [BufferGetPage](../B/BufferGetPage.md), BTPageGetOpaque (page access functions)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md) (gets highest offset number on page)
  - P_INCOMPLETE_SPLIT, P_RIGHTMOST, P_FIRSTDATAKEY (page flag and position checks)
  - [_bt_leftsib_splitflag](_bt_leftsib_splitflag.md) (checks if left sibling has incomplete split)
  - [_bt_relbuf](_bt_relbuf.md) (releases buffer locks)
  - [_bt_lock_subtree_parent](_bt_lock_subtree_parent.md) (recursive call to itself)
- Called from:
  - [_bt_mark_page_halfdead](_bt_mark_page_halfdead.md) (initiates subtree deletion process)
  - [_bt_lock_subtree_parent](_bt_lock_subtree_parent.md) (recursive calls)

## Notes and Other Information
- Returns false if deletion is unsafe at any level (preserves entire subtree)
- Returns true when a safe deletion boundary is found (locks subtree parent for caller)
- Implements recursive algorithm that may traverse multiple levels of the index
- Updates stack entries to reflect current downlink positions during processing
- Handles index corruption gracefully by logging warnings and aborting deletion
- Releases locks before recursive calls to avoid deadlocks, relying on leaf page lock for consistency
- Avoids completing incomplete splits to minimize disk space usage during VACUUM
- The function establishes both the height of the deletable subtree and provides the locked parent buffer needed for the actual deletion operation

## Simplified Source

```c
static bool _bt_lock_subtree_parent(Relation rel, Relation heaprel, BlockNumber child,
                                    BTStack stack, Buffer *subtreeparent,
                                    OffsetNumber *poffset, BlockNumber *topparent,
                                    BlockNumber *topparentrightsib)
{
    // Find and lock the parent page containing downlink to child
    Buffer pbuf = _bt_getstackbuf(rel, heaprel, stack, child);
    if (pbuf == InvalidBuffer) {
        // Index corruption - cannot find parent downlink
        ereport(LOG, "failed to re-find parent key for deletion target page %u", child);
        return false;
    }

    BlockNumber parent = stack->bts_blkno;
    OffsetNumber parentoffset = stack->bts_offset;
    Page page = BufferGetPage(pbuf);
    BTPageOpaque opaque = BTPageGetOpaque(page);
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

    // Check if child is not the rightmost child
    if (parentoffset < maxoff) {
        // Safe to delete - child is not rightmost
        *subtreeparent = pbuf;
        *poffset = parentoffset;
        return true;
    }

    // Child is rightmost - can only delete if parent can also be deleted
    Assert(parentoffset == maxoff);

    // Check if parent is also deletable (only child OR rightmost on level)
    if (parentoffset != P_FIRSTDATAKEY(opaque) || P_RIGHTMOST(opaque)) {
        // Parent has multiple children or is rightmost - unsafe
        _bt_relbuf(rel, pbuf);
        return false;
    }

    // Parent can be deleted - update top parent info and recurse
    *topparent = parent;
    *topparentrightsib = opaque->btpo_next;
    BlockNumber leftsibparent = opaque->btpo_prev;

    _bt_relbuf(rel, pbuf);

    // Check parent's left sibling for incomplete split
    if (_bt_leftsib_splitflag(rel, leftsibparent, parent))
        return false;

    // Recursively check if parent's parent allows deletion
    return _bt_lock_subtree_parent(rel, heaprel, parent, stack->bts_parent,
                                   subtreeparent, poffset,
                                   topparent, topparentrightsib);
}
```