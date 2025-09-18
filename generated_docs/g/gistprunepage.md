# gistprunepage

## Location
src/backend/access/gist/gist.c: 1671 - 1743

## Overview
Removes LP_DEAD (logically deleted) items from a GiST index leaf page to reclaim space and maintain index efficiency.

## Definition
```c
static void gistprunepage(Relation rel, Page page, Buffer buffer, Relation heapRel)
```

## Detailed Description
The `gistprunepage` function performs garbage collection on a GiST index leaf page by removing items that have been marked as LP_DEAD (logically deleted). This is an important maintenance operation that helps reclaim space and maintain index performance.

The function operates under the assumption that the buffer is exclusively locked, ensuring no concurrent modifications occur during the pruning process. It scans all items on the page to identify those marked with LP_DEAD flags, then removes them in a single atomic operation.

The function also handles Write-Ahead Logging (WAL) requirements for crash recovery and replication, computing snapshot conflict horizons when necessary for standby servers. After successful deletion, it clears the page's F_HAS_GARBAGE hint bit to indicate the page no longer contains dead items.

## Parameters / Member Variables
- `rel`: The GiST index relation being pruned
- `page`: The specific page within the index to be pruned (must be a leaf page)
- `buffer`: The buffer containing the page (must be exclusively locked)
- `heapRel`: The heap relation associated with the index, used for computing transaction horizons

## Dependencies
- Functions called/Symbols referenced:
  - GistPageIsLeaf
  - PageGetMaxOffsetNumber
  - PageGetItemId
  - ItemIdIsDead
  - XLogStandbyInfoActive
  - RelationNeedsWAL
  - index_compute_xid_horizon_for_tuples
  - PageIndexMultiDelete
  - GistClearPageHasGarbage
  - MarkBufferDirty
  - gistXLogDelete
  - gistGetFakeLSN
  - PageSetLSN
- Called from (representative examples):
  - gistplacetopage

## Notes and Other Information
- This is a static function, only accessible within the gist.c file
- Only operates on leaf pages (verified by GistPageIsLeaf assertion)
- Requires exclusive buffer lock to prevent concurrent modifications
- Handles WAL logging for crash recovery and replication consistency
- Uses critical sections to ensure atomic operations during page modification
- The F_HAS_GARBAGE hint bit may occasionally be falsely cleared, but this is acceptable as it's only a performance hint
- If no LP_DEAD items are found, the function still leaves the F_HAS_GARBAGE bit set rather than performing an additional write operation