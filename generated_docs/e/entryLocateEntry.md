# entryLocateEntry

## Location
[src/backend/access/gin/ginentrypage.c:270-345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginentrypage.c#L270-L345)

## Overview
Finds the correct tuple in a non-leaf GIN index page using binary search to locate the appropriate child page to descend to during index traversal.

## Definition

```c
static BlockNumber
entryLocateEntry(GinBtree btree, GinBtreeStack *stack)
```
## Detailed Description
This function performs a binary search on a non-leaf GIN index page to find the correct child page to descend to during index traversal. It operates on the assumption that the page has been correctly chosen and that the searching value should be present on the page. The function handles both full scan operations and targeted searches, comparing attribute entries using the GIN comparison functions to determine the correct downlink to follow.

For full scans, it simply returns the leftmost child page. For targeted searches, it performs a binary search algorithm, comparing the search key with entries on the page to find either an exact match or the appropriate position where the key should be located. The function properly handles the right infinity case when the search reaches the rightmost entry of a rightmost page.

## Parameters / Member Variables
- `btree`: GinBtree structure containing search parameters and callback functions for the current GIN index operation
- `*stack`: GinBtreeStack structure representing the current position in the index traversal, which will be updated with the offset of the located entry
## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - GinPageIsLeaf  
  - GinPageIsData
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - FirstOffsetNumber
  - GinPageRightMost
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [gintuple_get_attrnum](../g/gintuple_get_attrnum.md)
  - [gintuple_get_key](../g/gintuple_get_key.md)
  - [ginCompareAttEntries](../g/ginCompareAttEntries.md)
  - GinGetDownlink
- Called from (representative examples):
  - [ginPrepareEntryScan](../g/ginPrepareEntryScan.md)

## Notes and Other Information
- This is a static function internal to the GIN entry page implementation
- The function assumes the input page is a non-leaf, non-data page (verified by assertions)
- Uses binary search algorithm for efficient lookup in sorted index pages
- Handles special case of right infinity for rightmost pages
- Returns the block number of the child page to descend to next
- Updates the stack offset to point to the located entry for continued traversal

## Simplified Source

```c
// Simplified version of entryLocateEntry
static BlockNumber entryLocateEntry(GinBtree btree, GinBtreeStack *stack) {
    Page page = BufferGetPage(stack->buffer);

    // Handle full scan case: return leftmost child
    if (btree->fullScan) {
        stack->off = FirstOffsetNumber;
        stack->predictNumber *= PageGetMaxOffsetNumber(page);
        return btree->getLeftMostChild(btree, page);
    }

    // Binary search for target entry
    OffsetNumber low = FirstOffsetNumber;
    OffsetNumber high = PageGetMaxOffsetNumber(page) + 1;
    OffsetNumber maxoff = high - 1;

    while (high > low) {
        OffsetNumber mid = low + ((high - low) / 2);
        int result;

        if (mid == maxoff && GinPageRightMost(page)) {
            // Right infinity case
            result = -1;
        } else {
            // Compare with entry at mid position
            IndexTuple itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, mid));
            OffsetNumber attnum = gintuple_get_attrnum(btree->ginstate, itup);
            Datum key = gintuple_get_key(btree->ginstate, itup, &category);

            result = ginCompareAttEntries(btree->ginstate,
                                        btree->entryAttnum, btree->entryKey, btree->entryCategory,
                                        attnum, key, category);
        }

        if (result == 0) {
            // Found exact match
            stack->off = mid;
            return GinGetDownlink(itup);
        } else if (result > 0) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    // No exact match, use the position where key should be
    stack->off = high;
    IndexTuple itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, high));
    return GinGetDownlink(itup);
}
```

Key simplifications made:
- Removed detailed error handling assertions for clarity
- Consolidated variable declarations
- Added explanatory comments for main logic sections
- Simplified the binary search flow
- Focused on the core algorithm while maintaining correctness