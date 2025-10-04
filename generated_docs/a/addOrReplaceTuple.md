# addOrReplaceTuple

## Location
[src/backend/access/spgist/spgxlog.c:50-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgxlog.c#L50-L73)

## Overview
Adds a leaf tuple to a page or replaces an existing placeholder tuple, used to replay SpGistPageAddNewItem() operations during WAL recovery.

## Definition
```c
static void addOrReplaceTuple(Page page, Item tuple, int size, OffsetNumber offset)
```

## Detailed Description
This function handles the addition or replacement of tuples in SP-GiST index pages during WAL replay. It supports two distinct operations:

1. **Replacement**: If the specified offset points to an existing tuple, that tuple must be a placeholder tuple (SPGIST_PLACEHOLDER state). The function verifies this condition, decrements the placeholder counter, removes the old placeholder tuple, and then adds the new tuple.

2. **Addition**: If the offset is beyond the current maximum offset, the function simply adds the new tuple at the specified location.

The function includes safety checks to ensure data integrity, such as verifying that replaced tuples are actually placeholders and that the page can accommodate the new tuple at the specified offset.

## Parameters / Member Variables
- `page`: The target page where the tuple will be added or replaced
- `tuple`: The tuple data to be inserted into the page
- `size`: The size of the tuple in bytes
- `offset`: The offset number where the tuple should be placed on the page

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md) (get maximum offset number on page)
  - [PageGetItem](../P/PageGetItem.md) (retrieve item from page at offset)
  - [PageGetItemId](../P/PageGetItemId.md) (get item identifier for offset)
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md) (remove tuple from page)
  - PageAddItem (add new item to page)
  - SpGistPageGetOpaque (get SP-GiST specific page data)
  - elog (error logging)
  - Assert (assertion checking)
  - SpGistDeadTuple, Item, SPGIST_PLACEHOLDER (data types and constants)
- Called from (representative examples):
  - [spgRedoAddLeaf](../s/spgRedoAddLeaf.md)
  - [spgRedoMoveLeafs](../s/spgRedoMoveLeafs.md)
  - [spgRedoAddNode](../s/spgRedoAddNode.md)
  - [spgRedoSplitTuple](../s/spgRedoSplitTuple.md)
  - [spgRedoPickSplit](../s/spgRedoPickSplit.md)

## Notes and Other Information
- This is a static function used only within the SP-GiST WAL replay module (spgxlog.c)
- The function enforces strict rules about placeholder replacement to maintain index consistency
- Error conditions result in elog(ERROR) calls, which will abort the current transaction
- The function maintains the placeholder count in the page's opaque data structure
- Used extensively during SP-GiST index recovery operations to reconstruct the proper page state
- The PageAddItem call uses strict parameters (false, false) indicating no overwrite and no special handling

## Simplified Source

```c
static void
addOrReplaceTuple(Page page, Item tuple, int size, OffsetNumber offset)
{
    // Check if we're replacing an existing tuple
    if (offset <= PageGetMaxOffsetNumber(page)) {
        SpGistDeadTuple dt = (SpGistDeadTuple) PageGetItem(page, PageGetItemId(page, offset));

        // Verify the existing tuple is a placeholder
        if (dt->tupstate != SPGIST_PLACEHOLDER)
            elog(ERROR, "SPGiST tuple to be replaced is not a placeholder");

        // Update placeholder count and remove old tuple
        Assert(SpGistPageGetOpaque(page)->nPlaceholder > 0);
        SpGistPageGetOpaque(page)->nPlaceholder--;
        PageIndexTupleDelete(page, offset);
    }

    // Verify offset is valid for insertion
    Assert(offset <= PageGetMaxOffsetNumber(page) + 1);

    // Add the new tuple at the specified offset
    if (PageAddItem(page, tuple, size, offset, false, false) != offset)
        elog(ERROR, "failed to add item of size %u to SPGiST index page", size);
}
```