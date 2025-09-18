# dataIsMoveRight

## Location
[src/backend/access/gin/gindatapage.c:234-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L234-L251)

## Overview
Determines whether a GIN B-tree scan should follow the right link to continue searching for a target item pointer.

## Definition
```c
static bool dataIsMoveRight(GinBtree btree, Page page)
```

## Detailed Description
This static function implements the decision logic for right-link following during GIN B-tree traversal. It determines whether the search should continue to the right sibling page by comparing the target item pointer (stored in `btree->itemptr`) with the right boundary of the current page.

The function handles three key cases:
1. If the current page is the rightmost page, there's nowhere to go, so it returns false
2. If the current page is deleted, the search must move right to find valid data
3. Otherwise, it compares the target item pointer with the page's right bound to determine if the target could be on a page to the right

This function is part of the GIN B-tree navigation infrastructure and ensures efficient traversal by avoiding unnecessary page visits.

## Parameters / Member Variables
- `btree`: The GIN B-tree context containing the target item pointer to search for
- `page`: The current data page being examined

## Dependencies
- Functions called/Symbols referenced:
  - GinDataPageGetRightBound
  - GinPageRightMost
  - GinPageIsDeleted
  - [ginCompareItemPointers](../g/ginCompareItemPointers.md)
- Called from (representative examples):
  - [ginPrepareDataScan](../g/ginPrepareDataScan.md)

## Notes and Other Information
- This is a static function, only accessible within the same compilation unit
- Returns true if the search should continue to the right sibling page
- The function uses the page's right bound to make navigation decisions efficiently
- Deleted pages are always skipped by moving right
- Part of the GIN index B-tree traversal algorithm that ensures optimal search performance