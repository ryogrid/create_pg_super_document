# entryIsMoveRight

## Location
src/backend/access/gin/ginentrypage.c: 243 - 269

## Overview
Determines whether a GIN B-tree scan should move right to the next page by comparing the search key with the rightmost key on the current page.

## Definition
```c
static bool entryIsMoveRight(GinBtree btree, Page page)
```

## Detailed Description
The entryIsMoveRight function implements the "move right" logic for GIN entry tree traversal. It checks if the current page contains the target key by comparing the search key (stored in btree) with the rightmost key on the current page. If the search key is greater than the rightmost key, the scan should continue to the right sibling page. The function handles attribute numbers, key values, and null categories in the comparison.

## Parameters / Member Variables
- `btree`: GinBtree structure containing search context including target key, attribute number, and category
- `page`: Current page being examined during the scan

## Dependencies
- Functions called/Symbols referenced:
  - GinPageRightMost: Checks if the page is the rightmost page (no right sibling)
  - [getRightMostTuple](../g/getRightMostTuple.md): Gets the rightmost tuple from the page
  - [gintuple_get_attrnum](../g/gintuple_get_attrnum.md): Extracts attribute number from the tuple
  - [gintuple_get_key](../g/gintuple_get_key.md): Extracts key value and category from the tuple
  - [ginCompareAttEntries](../g/ginCompareAttEntries.md): Compares attribute entries considering attribute number, key, and category

- Called from (representative examples):
  - [ginPrepareEntryScan](../g/ginPrepareEntryScan.md): Preparing entry scan operations

## Notes and Other Information
- Function is static (internal to ginentrypage.c)
- Returns false immediately if the page is rightmost (no more pages to scan)
- Uses comprehensive comparison that considers attribute number, key value, and null category
- Essential for efficient navigation in GIN entry tree B-tree structure
- Part of the standard B-tree traversal pattern where pages are scanned left-to-right
- Comparison result > 0 indicates search key is greater than page boundary, requiring rightward movement