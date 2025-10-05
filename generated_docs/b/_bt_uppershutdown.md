# _bt_uppershutdown

## Location
[src/backend/access/nbtree/nbtsort.c:1063-1134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L1063-L1134)

## Overview
A function that completes the B-tree index construction by finalizing all levels, establishing the root page, and creating the metapage to make the index valid.

## Definition

```c
struct the metapage and make it
	 * point to the new root (unless we had no data at all, in which case it's
	 * set to point to "P_NONE").  This changes the index to the "valid" state
	 * by filling in a valid magic number in the metapage.
	 */
	metabuf = smgr_bulk_get_buf(wstate->bulkstate);
```
## Detailed Description
This function performs the final phase of B-tree index construction by traversing all levels of the partially constructed tree and completing the necessary operations to make it a valid, functional B-tree index.

The function operates in two main phases:

1. **Level Completion**: Iterates through each level of the tree from bottom to top, handling the last page at each level. For non-root levels, it adds entries to parent pages using low keys and establishes proper parent-child relationships. For the topmost level, it marks the page as the root.

2. **Metapage Creation**: Creates and initializes the metapage with the root block number and level information, which makes the index officially valid by setting the proper magic number.

Key operations performed:
- Links the last page on each level appropriately (either to parent or marks as root)
- Handles rightmost page adjustments by calling  to properly arrange the ItemId array
- Establishes proper B-tree structure by setting downlinks and parent relationships
- Creates a valid metapage pointing to the root, transitioning the index to a usable state

The function ensures that all pages are properly written out and that the tree structure maintains B-tree invariants throughout the finalization process.

## Parameters / Member Variables
- : BTWriteState structure containing the overall state of the index building operation, including bulk write state and index metadata
- : BTPageState structure representing the bottom level of the tree; the function traverses upward through linked BTPageState structures

## Dependencies
- Functions called/Symbols referenced:
  - BTPageGetOpaque
  - BTreeTupleGetNAtts
  - IndexRelationGetNumberOfKeyAttributes
  - P_LEFTMOST
  - [BTreeTupleSetDownLink](../B/BTreeTupleSetDownLink.md)
  - [_bt_buildadd](_bt_buildadd.md)
  - [pfree](../p/pfree.md)
  - [_bt_slideleft](_bt_slideleft.md)
  - [_bt_blwritepage](_bt_blwritepage.md)
  - [smgr_bulk_get_buf](../s/smgr_bulk_get_buf.md)
  - [_bt_initmetapage](_bt_initmetapage.md)
  - BTP_ROOT
  - P_NONE
  - BTREE_METAPAGE
- Called from (representative examples):
  - [_bt_load](_bt_load.md)

## Notes and Other Information
- This function marks the transition point where the index becomes valid and usable
- The metapage creation is crucial as it contains the magic number that identifies the file as a valid B-tree index
- Proper handling of rightmost pages is essential for maintaining B-tree search semantics
- The function handles both empty indexes (root points to P_NONE) and populated indexes
- Memory management includes proper cleanup of BTPageState lowkey pointers
- The allequalimage flag in the metapage affects whether the index supports certain optimization techniques
- After this function completes, the index is ready for normal operation and can handle inserts, updates, and queries

## Simplified Source

```c
static void
_bt_uppershutdown(BTWriteState *wstate, BTPageState *state)
{
    BTPageState *s;
    BlockNumber rootblkno = P_NONE;
    uint32 rootlevel = 0;

    // Complete each level of the tree from bottom to top
    for (s = state; s != NULL; s = s->btps_next) {
        BlockNumber blkno = s->btps_blkno;
        BTPageOpaque opaque = BTPageGetOpaque((Page) s->btps_buf);

        // Handle top level - mark as root
        if (s->btps_next == NULL) {
            opaque->btpo_flags |= BTP_ROOT;
            rootblkno = blkno;
            rootlevel = s->btps_level;
        }
        // Handle intermediate levels - add entry to parent
        else {
            BTreeTupleSetDownLink(s->btps_lowkey, blkno);
            _bt_buildadd(wstate, s->btps_next, s->btps_lowkey, 0);
            pfree(s->btps_lowkey);
            s->btps_lowkey = NULL;
        }

        // Finalize rightmost page and write it out
        _bt_slideleft((Page) s->btps_buf);
        _bt_blwritepage(wstate, s->btps_buf, s->btps_blkno);
        s->btps_buf = NULL;
    }

    // Create metapage to make index valid
    BulkWriteBuffer metabuf = smgr_bulk_get_buf(wstate->bulkstate);
    _bt_initmetapage((Page) metabuf, rootblkno, rootlevel,
                     wstate->inskey->allequalimage);
    _bt_blwritepage(wstate, metabuf, BTREE_METAPAGE);
}
```