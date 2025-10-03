# gistformdownlink

## Location
[src/backend/access/gist/gist.c:1135-1194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gist.c#L1135-L1194)

## Overview
Creates a downlink index tuple that represents all entries on a given page, used when inserting references to child pages in internal nodes of the GiST tree.

## Definition

```c
struct that. So we just use the downlink of
	 * the original page that was split - that's as far from optimal as it can
	 * get but will do..
	 */
	if (!downlink)
	{
		ItemId		iid;

		LockBuffer(stack->parent->buffer, GIST_EXCLUSIVE);
		gistFindCorrectParent(rel, stack, is_build);
		iid = PageGetItemId(stack->parent->page, stack->downlinkoffnum);
		downlink = (IndexTuple) PageGetItem(stack->parent->page, iid);
		downlink = CopyIndexTuple(downlink);
		LockBuffer(stack->parent->buffer, GIST_UNLOCK);
	}

	ItemPointerSetBlockNumber(&(downlink->t_tid), BufferGetBlockNumber(buf));
```
## Detailed Description
 constructs a downlink tuple that will be inserted into a parent page to reference a child page. The function works by:

1. **Union Computation**: Iterates through all tuples on the target page and computes their union using . This creates a bounding key that covers all entries on the child page.

2. **Empty Page Handling**: For completely empty pages, constructs a downlink by copying the original downlink from the parent page. This ensures the downlink is consistent with the parent's constraints while potentially being suboptimal for query performance.

3. **Tuple Finalization**: Sets the block number to point to the target buffer and marks the tuple as valid.

The union computation is essential for maintaining the GiST tree property that parent keys properly bound their children. When pages are split, new downlinks must be created for the resulting pages.

## Parameters / Member Variables
- `r`: The GiST index relation
- `buf`: Buffer containing the page for which to create a downlink
- `giststate`: GiST-specific state information including operator classes
- `stack`: Insertion stack used to locate parent information when needed for empty pages
- `is_build`: Boolean indicating whether this is called during index build

## Dependencies
- Functions called/Symbols referenced:
  - [gistgetadjusted](gistgetadjusted.md)
  - [gistFindCorrectParent](gistFindCorrectParent.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [CopyIndexTuple](../C/CopyIndexTuple.md)
  - [ItemPointerSetBlockNumber](../I/ItemPointerSetBlockNumber.md)
  - GistTupleSetValid
  - [LockBuffer](../L/LockBuffer.md)
- Called from (representative examples):
  - [gistfixsplit](gistfixsplit.md)

## Notes and Other Information
- The function handles the special case of empty pages by reusing the parent's downlink, which is suboptimal but ensures correctness
- The union computation ensures that the resulting downlink properly bounds all entries on the child page
- Proper locking is used when accessing parent page information for empty page handling
- The resulting tuple has its block pointer set to reference the child page and is marked as valid
- This function is critical for maintaining the bounding property of GiST trees after page splits
- The choice to reuse parent downlinks for empty pages prioritizes correctness over optimality

## Simplified Source
```c
static IndexTuple
gistformdownlink(Relation rel, Buffer buf, GISTSTATE *giststate,
                 GISTInsertStack *stack, bool is_build) {
    Page page = BufferGetPage(buf);
    IndexTuple downlink = NULL;

    // Compute union of all tuples on the page
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    for (OffsetNumber offset = FirstOffsetNumber; offset <= maxoff; offset++) {
        IndexTuple ituple = (IndexTuple)
            PageGetItem(page, PageGetItemId(page, offset));

        if (downlink == NULL) {
            // First tuple becomes base downlink
            downlink = CopyIndexTuple(ituple);
        } else {
            // Compute union with existing downlink
            IndexTuple newdownlink = gistgetadjusted(rel, downlink, ituple,
                                                    giststate);
            if (newdownlink)
                downlink = newdownlink;
        }
    }

    // Handle empty page case
    if (!downlink) {
        // For empty pages, copy the parent's downlink
        // This is suboptimal but ensures correctness
        LockBuffer(stack->parent->buffer, GIST_EXCLUSIVE);
        gistFindCorrectParent(rel, stack, is_build);

        ItemId iid = PageGetItemId(stack->parent->page, stack->downlinkoffnum);
        downlink = (IndexTuple) PageGetItem(stack->parent->page, iid);
        downlink = CopyIndexTuple(downlink);

        LockBuffer(stack->parent->buffer, GIST_UNLOCK);
    }

    // Set downlink to point to this page
    ItemPointerSetBlockNumber(&(downlink->t_tid), BufferGetBlockNumber(buf));
    GistTupleSetValid(downlink);

    return downlink;
}
```