# dataLocateItem

## Location
src/backend/access/gin/gindatapage.c: 252 - 318

## Overview
Locates the correct PostingItem in a non-leaf GIN data page using binary search to find the appropriate child page to descend to.

## Definition
```c
static BlockNumber dataLocateItem(GinBtree btree, GinBtreeStack *stack)
```

## Detailed Description
This static function implements binary search to locate the correct PostingItem in a non-leaf GIN data page. The function determines which child page should be visited next during B-tree traversal by comparing the target item pointer with the keys stored in PostingItems on the current page.

The function handles two main scenarios:
1. **Full scan mode**: When `btree->fullScan` is true, it returns the leftmost child page and updates prediction counters
2. **Targeted search**: Uses binary search to find the appropriate PostingItem whose key range contains the target item pointer

The binary search algorithm handles the special case where the rightmost PostingItem represents "right infinity" - meaning it covers all values greater than the previous item's key. The function maintains the search bounds and updates the stack offset to indicate which PostingItem was selected.

## Parameters / Member Variables
- `btree`: The GIN B-tree context containing the target item pointer and scan configuration
- `stack`: The current B-tree stack frame, updated with the selected offset

## Dependencies
- Functions called/Symbols referenced:
  - BufferGetPage
  - GinPageIsLeaf
  - GinPageIsData
  - GinPageGetOpaque
  - GinDataPageGetPostingItem
  - ginCompareItemPointers
  - PostingItemGetBlockNumber
- Called from (representative examples):
  - ginPrepareDataScan

## Notes and Other Information
- This is a static function, only accessible within the same compilation unit
- Returns the block number of the child page that should be visited next
- Uses assertions to ensure the page is a non-leaf data page
- The binary search handles the "right infinity" case for the rightmost PostingItem
- Updates the stack offset to indicate which PostingItem was selected for descent
- In full scan mode, multiplies prediction numbers by the page's maxoff for cardinality estimation
- The function assumes the current page is correct and that the searched value should be found on this page