# getRightMostTuple

## Location
[src/backend/access/gin/ginentrypage.c:235-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginentrypage.c#L235-L242)

## Overview
Retrieves the rightmost tuple from a GIN entry page, used instead of right bounds since entry trees are static structures.

## Definition
```c
static IndexTuple getRightMostTuple(Page page)
```

## Detailed Description
The getRightMostTuple function returns the rightmost (last) tuple on a GIN entry page. This function is specifically designed for GIN entry trees, which are static structures where tuples are never deleted. Instead of maintaining right boundary information like other B-tree implementations, GIN uses the rightmost key for boundary operations and comparisons.

## Parameters / Member Variables
- `page`: The page from which to retrieve the rightmost tuple

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md): Gets the maximum offset number (last item) on the page
  - [PageGetItemId](../P/PageGetItemId.md): Gets the item identifier for a specific offset
  - [PageGetItem](../P/PageGetItem.md): Retrieves the actual item (tuple) from the page using the item identifier

- Called from (representative examples):
  - [entryIsMoveRight](../e/entryIsMoveRight.md): Checking if entry operations should move right
  - [entryPrepareDownlink](../e/entryPrepareDownlink.md): Preparing downlinks during page operations
  - [ginEntryFillRoot](ginEntryFillRoot.md): Filling root page entries

## Notes and Other Information
- Function is static (internal to ginentrypage.c)
- Designed specifically for GIN entry tree static structure where deletions do not occur
- Used as an alternative to traditional B-tree right bound mechanisms
- Simple implementation that directly accesses the last item on the page
- Essential for navigation and boundary checking in GIN entry tree operations