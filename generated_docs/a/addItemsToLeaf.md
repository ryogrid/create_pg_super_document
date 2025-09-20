# addItemsToLeaf

## Location
[src/backend/access/gin/gindatapage.c:1444-1570](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L1444-L1570)

## Overview
addItemsToLeaf distributes new ItemPointer items to the appropriate segments within a disassembled GIN data leaf page, merging them with existing items and handling duplicates.

## Definition

```c
struct one new segment to hold
	 * all the new items.
	 */
	if (dlist_is_empty(&leaf->segments))
	{
		newseg = palloc(sizeof(leafSegmentInfo));
		newseg->seg = NULL;
		newseg->items = newItems;
		newseg->nitems = nNewItems;
		newseg->action = GIN_SEGMENT_INSERT;
		dlist_push_tail(&leaf->segments, &newseg->node);
		return true;
	}

	dlist_foreach(iter, &leaf->segments)
	{
		leafSegmentInfo *cur = (leafSegmentInfo *) dlist_container(leafSegmentInfo, node, iter.cur);
		int			nthis;
		ItemPointer tmpitems;
		int			ntmpitems;

		/*
		 * How many of the new items fall into this segment?
		 */
		if (!dlist_has_next(&leaf->segments, iter.cur))
			nthis = newleft;
		else
		{
			leafSegmentInfo *next;
			ItemPointerData next_first;

			next = (leafSegmentInfo *) dlist_container(leafSegmentInfo, node,
													   dlist_next_node(&leaf->segments, iter.cur));
			if (next->items)
				next_first = next->items[0];
			else
			{
				Assert(next->seg != NULL);
				next_first = next->seg->first;
			}

			nthis = 0;
			while (nthis < newleft && ginCompareItemPointers(&nextnew[nthis], &next_first) < 0)
				nthis++;
		}
		if (nthis == 0)
			continue;

		/* Merge the new items with the existing items. */
		if (!cur->items)
			cur->items = ginPostingListDecode(cur->seg, &cur->nitems);

		/*
		 * Fast path for the important special case that we're appending to
		 * the end of the page: don't let the last segment on the page grow
		 * larger than the target, create a new segment before that happens.
		 */
		if (!dlist_has_next(&leaf->segments, iter.cur) &&
			ginCompareItemPointers(&cur->items[cur->nitems - 1], &nextnew[0]) < 0 &&
			cur->seg != NULL &&
			SizeOfGinPostingList(cur->seg) >= GinPostingListSegmentTargetSize)
		{
			newseg = palloc(sizeof(leafSegmentInfo));
			newseg->seg = NULL;
			newseg->items = nextnew;
			newseg->nitems = nthis;
			newseg->action = GIN_SEGMENT_INSERT;
			dlist_push_tail(&leaf->segments, &newseg->node);
			modified = true;
			break;
		}

		tmpitems = ginMergeItemPointers(cur->items, cur->nitems,
										nextnew, nthis,
										&ntmpitems);
		if (ntmpitems != cur->nitems)
		{
			/*
			 * If there are no duplicates, track the added items so that we
			 * can emit a compact ADDITEMS WAL record later on. (it doesn't
			 * seem worth re-checking which items were duplicates, if there
			 * were any)
			 */
			if (ntmpitems == nthis + cur->nitems &&
				cur->action == GIN_SEGMENT_UNMODIFIED)
			{
				cur->action = GIN_SEGMENT_ADDITEMS;
				cur->modifieditems = nextnew;
				cur->nmodifieditems = nthis;
			}
			else
				cur->action = GIN_SEGMENT_REPLACE;

			cur->items = tmpitems;
			cur->nitems = ntmpitems;
			cur->seg = NULL;
			modified = true;
		}

		nextnew += nthis;
		newleft -= nthis;
		if (newleft == 0)
			break;
	}

	return modified;
```
## Detailed Description
This static function efficiently distributes new ItemPointer items across the segments of a disassembled leaf page. The function iterates through the leaf's segments and determines which new items belong to each segment based on their sort order. For each affected segment, it:

1. Decodes the segment's existing items if they haven't been decoded already
2. Merges the new items with existing items, removing duplicates
3. Updates the segment's action flag to indicate the type of modification
4. Implements a fast path optimization for appending to the end of the page

The function includes special handling for empty pages (creates a single new segment) and implements segment size management to prevent segments from growing too large by creating new segments when necessary.

Returns true if any new items were actually added (not all duplicates), false if all items were duplicates.

## Parameters / Member Variables
- `leaf`: The disassembledLeaf structure to modify
- `newItems`: Array of new ItemPointer items to add
- `nNewItems`: Number of new items in the newItems array

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - [palloc](../p/palloc.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - dlist_foreach
  - dlist_container
  - [dlist_has_next](../d/dlist_has_next.md)
  - [dlist_next_node](../d/dlist_next_node.md)
  - [ginCompareItemPointers](../g/ginCompareItemPointers.md)
  - [ginPostingListDecode](../g/ginPostingListDecode.md)
  - SizeOfGinPostingList
  - [ginMergeItemPointers](../g/ginMergeItemPointers.md)
  - GIN_SEGMENT_INSERT
  - GIN_SEGMENT_UNMODIFIED
  - GIN_SEGMENT_ADDITEMS
  - GIN_SEGMENT_REPLACE
  - GinPostingListSegmentTargetSize
- Called from (representative examples):
  - [dataBeginPlaceToPageLeaf](../d/dataBeginPlaceToPageLeaf.md)

## Notes and Other Information
- This is a static function, only accessible within gindatapage.c
- Handles three types of segment actions: INSERT (new segment), ADDITEMS (items added to existing segment), REPLACE (segment completely reconstructed)
- Implements an optimization for appending items to avoid creating oversized segments
- Uses efficient merging algorithms to handle duplicate detection and removal
- The function modifies the disassembledLeaf structure in place
- Maintains sorted order of items within segments
- Located in src/backend/access/gin/gindatapage.c at lines 1444-1570
- Part of the GIN index insertion and update infrastructure