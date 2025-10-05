# _bt_search

## Location
[src/backend/access/nbtree/nbtsearch.c:96-234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsearch.c#L96-L234)

## Overview
This function searches the B-tree for a particular scankey or more precisely the first leaf page where the key could be located, returning a stack of parent-page pointers.

## Definition

```c
BTStack
_bt_search(Relation rel, Relation heaprel, BTScanInsert key, Buffer *bufP,
		   int access)
```
## Detailed Description
_bt_search implements the core B-tree search algorithm that traverses from the root to a leaf page. It uses an insertion-type scankey to find the appropriate leaf page where a key could be located. The function handles both read and write access modes, with write mode allowing for completion of incomplete splits encountered during traversal and creation of empty root pages.

The search proceeds level by level from the root, using binary search at each internal page to find the appropriate child pointer. At each level, it may need to move right if the page has split since its downlink was read from the parent. The function maintains a stack of parent page positions that can be used later for insertions or deletions.

The returned buffer is locked according to the access parameter, and in write mode, any incomplete splits are finished during traversal.

## Parameters / Member Variables
- `rel`: The B-tree index relation being searched
- `heaprel`: The heap relation (required for BT_WRITE access for potential root page allocation)
- `key`: BTScanInsert structure containing the search key (insertion-type scankey)
- `*bufP`: Pointer to Buffer where the leaf page buffer will be returned (locked and pinned)
- `access`: Access mode (BT_READ or BT_WRITE) determining locking behavior and split completion
## Dependencies
- Functions called/Symbols referenced:
  - [_bt_getroot](_bt_getroot.md)
  - [_bt_moveright](_bt_moveright.md)
  - [_bt_binsrch](_bt_binsrch.md)
  - [_bt_relandgetbuf](_bt_relandgetbuf.md)
  - [_bt_unlockbuf](_bt_unlockbuf.md)
  - [_bt_lockbuf](_bt_lockbuf.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - BTPageGetOpaque
  - P_ISLEAF
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [BTreeTupleIsPivot](../B/BTreeTupleIsPivot.md)
  - [BTreeTupleGetDownLink](../B/BTreeTupleGetDownLink.md)
- Called from (representative examples):
  - [_bt_search_insert](_bt_search_insert.md)
  - [_bt_pagedel](_bt_pagedel.md)
  - [_bt_first](_bt_first.md)

## Notes and Other Information
The function implements several important concurrency considerations:
1. Uses _bt_moveright to handle page splits that may have occurred since reading downlinks
2. In write mode, completes incomplete splits encountered during traversal
3. For single-page indexes (root is leaf), special handling ensures proper write locking
4. Returns a stack of parent positions but no entry for the leaf level itself
5. In BT_READ mode with empty index, returns InvalidBuffer rather than creating pages
6. The heaprel parameter is required for BT_WRITE access since root page allocation may be needed

## Simplified Source

```c
BTStack
_bt_search(Relation rel, Relation heaprel, BTScanInsert key, Buffer *bufP, int access)
{
    BTStack stack_in = NULL;
    int page_access = BT_READ;

    // Get the root page to start search
    *bufP = _bt_getroot(rel, heaprel, access);

    // Handle empty index for read access
    if (!BufferIsValid(*bufP))
        return (BTStack) NULL;

    // Traverse tree level by level until we reach a leaf
    for (;;)
    {
        Page page;
        BTPageOpaque opaque;
        OffsetNumber offnum;
        ItemId itemid;
        IndexTuple itup;
        BlockNumber child;
        BTStack new_stack;

        // Handle page splits that may have occurred since reading downlink
        *bufP = _bt_moveright(rel, heaprel, key, *bufP, (access == BT_WRITE),
                             stack_in, page_access);

        // Check if we've reached a leaf page
        page = BufferGetPage(*bufP);
        opaque = BTPageGetOpaque(page);
        if (P_ISLEAF(opaque))
            break;

        // Find appropriate pivot tuple pointing to child page
        offnum = _bt_binsrch(rel, key, *bufP);
        itemid = PageGetItemId(page, offnum);
        itup = (IndexTuple) PageGetItem(page, itemid);
        child = BTreeTupleGetDownLink(itup);

        // Save this page's position in the stack for potential splits
        new_stack = (BTStack) palloc(sizeof(BTStackData));
        new_stack->bts_blkno = BufferGetBlockNumber(*bufP);
        new_stack->bts_offset = offnum;
        new_stack->bts_parent = stack_in;

        // If next level is leaf and we need write access, prepare for it
        if (opaque->btpo_level == 1 && access == BT_WRITE)
            page_access = BT_WRITE;

        // Move to child page
        *bufP = _bt_relandgetbuf(rel, *bufP, child, page_access);
        stack_in = new_stack;
    }

    // Handle special case: root page is also leaf and we need write lock
    if (access == BT_WRITE && page_access == BT_READ)
    {
        _bt_unlockbuf(rel, *bufP);
        _bt_lockbuf(rel, *bufP, BT_WRITE);

        // Check for splits after lock upgrade
        *bufP = _bt_moveright(rel, heaprel, key, *bufP, true, stack_in, BT_WRITE);
    }

    return stack_in;
}
```