# scanPostingTree

## Location
src/backend/access/gin/ginget.c: 69 - 120

## Overview
This is a static function that scans all pages of a GIN posting tree and extracts all heap ItemPointers, storing them in the scan entry's match bitmap for efficient query processing.

## Definition
```c
static void scanPostingTree(Relation index, GinScanEntry scanEntry, BlockNumber rootPostingTree)
```

## Detailed Description
The function performs a complete traversal of a GIN posting tree starting from the root block number. It begins by descending to the leftmost leaf page using `ginScanBeginPostingTree`, then iterates through all leaf pages from left to right. For each non-deleted leaf page, it extracts all ItemPointers using `GinDataLeafPageGetItemsToTbm` and adds them to the scan entry's match bitmap. The function also maintains a count of predicted results for query optimization purposes.

The traversal continues until it reaches the rightmost page of the posting tree, ensuring all relevant ItemPointers are collected. This is a key component of GIN index scanning that enables efficient bitmap-based query execution.

## Parameters / Member Variables
- `index`: The GIN index relation being scanned
- `scanEntry`: Pointer to GinScanEntry structure that holds the match bitmap and scan state
- `rootPostingTree`: Block number of the root page of the posting tree to scan

## Dependencies
- Functions called/Symbols referenced:
  - ginScanBeginPostingTree
  - IncrBufferRefCount
  - freeGinBtreeStack
  - GinPageGetOpaque
  - GinDataLeafPageGetItemsToTbm
  - GinPageRightMost
  - ginStepRight
  - UnlockReleaseBuffer
  - GIN_DELETED (flag constant)
  - GIN_SHARE (lock mode constant)
- Called from:
  - collectMatchBitmap (src/backend/access/gin/ginget.c:247)

## Notes and Other Information
- This is a static function, only accessible within the ginget.c file
- The function handles buffer reference counting carefully to prevent premature unpinning
- Skips deleted pages during the scan to avoid processing stale data
- Updates the `predictNumberResult` counter for query optimization
- Part of the GIN index bitmap collection infrastructure
- Traverses posting trees in a left-to-right manner, ensuring all leaf pages are processed
- Uses shared locks (GIN_SHARE) for read operations to allow concurrent access