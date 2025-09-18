# entryLoadMoreItems

## Location
src/backend/access/gin/ginget.c: 655 - 792

## Overview
Loads the next batch of item pointers from a GIN posting tree, implementing efficient page navigation strategies and handling page boundaries for continuous scanning.

## Definition


## Detailed Description
The entryLoadMoreItems function is a critical component of GIN index scanning that manages the incremental loading of item pointers from posting trees. It implements two distinct navigation strategies to efficiently locate the next batch of items: stepping right to adjacent pages when the next item should be on the immediate next page, or re-descending from the root when larger jumps are needed.

The function handles complex scenarios including page splits, concurrent deletions, and lossy page pointers. When stepping right, it follows the right-link chain until finding a page containing items greater than the advancePast pointer. When re-descending, it calculates the appropriate search key based on whether the advancePast pointer represents a lossy page or a specific item location.

The function maintains proper buffer management by pinning pages to prevent vacuum interference while unlocking them to avoid blocking other operations. It copies items from pages into the entry's local list array, enabling continued processing even after releasing page locks.

## Parameters / Member Variables
- : Pointer to GIN state containing index metadata and configuration
- : GIN scan entry being processed, containing buffer, position, and item list state
- : Item pointer indicating the position to advance beyond when loading new items

## Dependencies
- Functions called/Symbols referenced:
  - [ginCompareItemPointers](../g/ginCompareItemPointers.md)
  - [ginFindLeafPage](../g/ginFindLeafPage.md)
  - [ginStepRight](../g/ginStepRight.md)
  - [GinDataLeafPageGetItems](../G/GinDataLeafPageGetItems.md)
  - GinDataPageGetRightBound
  - ItemPointerIsLossyPage
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - GinItemPointerGetBlockNumber
  - GinItemPointerGetOffsetNumber
  - [BufferGetPage](../B/BufferGetPage.md)
  - [LockBuffer](../L/LockBuffer.md)/UnlockReleaseBuffer
  - [IncrBufferRefCount](../I/IncrBufferRefCount.md)
  - [freeGinBtreeStack](../f/freeGinBtreeStack.md)
- Data types used:
  - [GinState](../G/GinState.md)
  - [GinScanEntry](../G/GinScanEntry.md)
  - [ItemPointerData](../I/ItemPointerData.md)
  - [GinBtreeStack](../G/GinBtreeStack.md)
  - Page
- Constants:
  - GIN_SHARE, GIN_UNLOCK
  - GIN_DELETED
  - InvalidOffsetNumber
  - FirstOffsetNumber
- Called from:
  - [entryGetItem](entryGetItem.md)

## Notes and Other Information
- Uses a hybrid navigation strategy: step-right for sequential access, re-descent for random access
- Handles lossy page pointers specially by advancing to the next block's first offset
- Implements sophisticated page boundary detection and right-link following
- Manages concurrent vacuum scenarios by detecting and skipping deleted pages
- Critical for performance as it determines how efficiently large posting lists are traversed
- The copied items approach allows continued processing while releasing page locks, improving concurrency
- Debug logging helps track page navigation decisions during development and troubleshooting