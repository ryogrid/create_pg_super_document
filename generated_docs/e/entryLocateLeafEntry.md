# entryLocateLeafEntry

## Location
[src/backend/access/gin/ginentrypage.c:346-404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginentrypage.c#L346-L404)

## Overview
Searches for the correct position of a value on a GIN index leaf page using binary search, returning whether the exact value was found.

## Definition

```c
static bool
entryLocateLeafEntry(GinBtree btree, GinBtreeStack *stack)
```
## Detailed Description
This function performs a binary search on a leaf page of a GIN index to locate the correct position for a specific entry value. Unlike entryLocateEntry which works on non-leaf pages to find child pages, this function operates on leaf pages to find actual data entries. It assumes the page has been correctly chosen during the index traversal.

The function returns true if an exact match is found, and false otherwise. In both cases, it updates the stack offset to point to the appropriate position: either the exact match location or the insertion point where the value should be placed. For full scans, it simply sets the offset to the first position and returns true.

## Parameters / Member Variables
- : GinBtree structure containing search parameters including the entry key, attribute number, and category being searched for
- : GinBtreeStack structure representing the current leaf page position, which will be updated with the offset of the located or insertion position

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - GinPageIsLeaf
  - GinPageIsData
  - FirstOffsetNumber
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [gintuple_get_attrnum](../g/gintuple_get_attrnum.md)
  - [gintuple_get_key](../g/gintuple_get_key.md)
  - [ginCompareAttEntries](../g/ginCompareAttEntries.md)
- Called from (representative examples):
  - [ginPrepareEntryScan](../g/ginPrepareEntryScan.md)

## Notes and Other Information
- This is a static function internal to the GIN entry page implementation
- The function assumes the input page is a leaf, non-data page (verified by assertions)
- Uses binary search for efficient lookup in sorted leaf pages
- Returns boolean indicating exact match found (true) or not found (false)
- Always updates stack offset to the correct position regardless of match result
- For empty pages (high < low), sets offset to FirstOffsetNumber and returns false
- Critical for both search operations and determining insertion points for new entries

## Simplified Source

```c
// Simplified version of entryLocateLeafEntry
static bool entryLocateLeafEntry(GinBtree btree, GinBtreeStack *stack) {
    Page page = BufferGetPage(stack->buffer);

    // Handle full scan case
    if (btree->fullScan) {
        stack->off = FirstOffsetNumber;
        return true;
    }

    OffsetNumber low = FirstOffsetNumber;
    OffsetNumber high = PageGetMaxOffsetNumber(page);

    // Handle empty page
    if (high < low) {
        stack->off = FirstOffsetNumber;
        return false;
    }

    high++;

    // Binary search for exact entry
    while (high > low) {
        OffsetNumber mid = low + ((high - low) / 2);

        // Get entry at mid position and compare
        IndexTuple itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, mid));
        OffsetNumber attnum = gintuple_get_attrnum(btree->ginstate, itup);
        Datum key = gintuple_get_key(btree->ginstate, itup, &category);

        int result = ginCompareAttEntries(btree->ginstate,
                                        btree->entryAttnum, btree->entryKey, btree->entryCategory,
                                        attnum, key, category);

        if (result == 0) {
            // Found exact match
            stack->off = mid;
            return true;
        } else if (result > 0) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    // No exact match found, set insertion point
    stack->off = high;
    return false;
}
```

Key simplifications made:
- Removed assertions for clarity
- Added explanatory comments for each major section
- Consolidated variable declarations within the search loop
- Emphasized the boolean return value meaning (exact match vs insertion point)
- Simplified the control flow while preserving the binary search algorithm