# _bt_get_endpoint

## Location
[src/backend/access/nbtree/nbtsearch.c:2492-2572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsearch.c#L2492-L2572)

## Overview
Finds the first or last page at a specified level in a B-tree index, providing the foundation for endpoint-based tree operations and traversals.

## Definition

```c
Buffer
_bt_get_endpoint(Relation rel, uint32 level, bool rightmost)
```
## Detailed Description
This function locates either the leftmost or rightmost page at a specified level within a B-tree index structure. It implements a top-down traversal strategy, starting from either the fast root (for leaf level operations) or the true root (for internal level operations). The function handles various edge cases including deleted pages, page splits, and index corruption scenarios.

The algorithm ensures that only live pages are returned by stepping right when encountering deleted or ignored pages. For rightmost searches, it continues stepping right until reaching the actual rightmost page, accounting for concurrent page splits. The function performs level validation and provides appropriate error handling for corrupted index structures.

## Parameters / Member Variables
- `rel`: Relation - The B-tree index relation to search within
- `level`: uint32 - The tree level to search (0 for leaf level, higher numbers for internal levels)
- `rightmost`: bool - If true, finds the rightmost page; if false, finds the leftmost page
## Dependencies
- Functions called/Symbols referenced:
  - [_bt_getroot](_bt_getroot.md)
  - [_bt_gettrueroot](_bt_gettrueroot.md)
  - [_bt_relandgetbuf](_bt_relandgetbuf.md)
  - BTPageGetOpaque
  - P_IGNORE
  - P_RIGHTMOST
  - P_FIRSTDATAKEY
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [BTreeTupleGetDownLink](../B/BTreeTupleGetDownLink.md)
  - BTPageOpaque (type)
  - BT_READ (constant)
  - P_NONE (constant)
- Called from (representative examples):
  - [_bt_insert_parent](_bt_insert_parent.md)
  - [_bt_endpoint](_bt_endpoint.md)

## Notes and Other Information
- Returns InvalidBuffer if the index is empty, otherwise always returns a valid, live page
- The returned buffer is pinned and read-locked
- Uses fast root for leaf-level searches and true root for internal levels for optimization
- Implements robust error checking for index corruption with detailed error messages
- Handles concurrent operations gracefully by stepping right when necessary
- For leaf level (level 0), descends to the leftmost or rightmost child at each internal level
- Provides the foundation for other endpoint-related operations in the B-tree implementation
- This is not a static function, making it accessible from other source files in the B-tree subsystem

## Simplified Source

```c
Buffer
_bt_get_endpoint(Relation rel, uint32 level, bool rightmost)
{
    Buffer buf;
    Page page;
    BTPageOpaque opaque;
    OffsetNumber offnum;
    BlockNumber blkno;
    IndexTuple itup;

    // Start from appropriate root
    if (level == 0)
        buf = _bt_getroot(rel, NULL, BT_READ);  // Fast root for leaf
    else
        buf = _bt_gettrueroot(rel);  // True root for internal levels

    if (!BufferIsValid(buf))
        return InvalidBuffer;

    page = BufferGetPage(buf);
    opaque = BTPageGetOpaque(page);

    for (;;) {
        // Skip deleted pages and find rightmost if requested
        while (P_IGNORE(opaque) || (rightmost && !P_RIGHTMOST(opaque))) {
            blkno = opaque->btpo_next;
            if (blkno == P_NONE)
                elog(ERROR, "fell off the end of index \"%s\"",
                     RelationGetRelationName(rel));

            buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
            page = BufferGetPage(buf);
            opaque = BTPageGetOpaque(page);
        }

        // Check if we've reached the target level
        if (opaque->btpo_level == level)
            break;

        if (opaque->btpo_level < level)
            ereport(ERROR,
                    (errcode(ERRCODE_INDEX_CORRUPTED),
                     errmsg_internal("btree level %u not found in index \"%s\"",
                                     level, RelationGetRelationName(rel))));

        // Descend to appropriate child page
        if (rightmost)
            offnum = PageGetMaxOffsetNumber(page);
        else
            offnum = P_FIRSTDATAKEY(opaque);

        itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offnum));
        blkno = BTreeTupleGetDownLink(itup);

        buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
        page = BufferGetPage(buf);
        opaque = BTPageGetOpaque(page);
    }

    return buf;
}
```