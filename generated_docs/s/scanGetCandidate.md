# scanGetCandidate

## Location
[src/backend/access/gin/ginget.c:1454-1540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginget.c#L1454-L1540)

## Overview
Retrieves the next heap row's ItemPointer to be checked from the GIN pending list, handling page transitions and setting offset ranges for the row's tuples.

## Definition
```c
static bool scanGetCandidate(IndexScanDesc scan, pendingPosition *pos)
```

## Detailed Description
This function navigates through the GIN pending list to find the next heap row that needs to be processed during index scans. It manages the complexities of pending list pagination, where a single heap row's entries may span multiple pages. The function sets up position information including the range of offsets (firstOffset to lastOffset) that contain tuples belonging to the current heap row.

When the current page is exhausted, it automatically advances to the next page in the pending list chain, properly handling buffer locking to prevent concurrent deletion by cleanup processes. The function distinguishes between pages with full row data versus partial row data, adjusting the offset range accordingly.

## Parameters / Member Variables
- `scan`: Index scan descriptor providing access to the index relation
- `pos`: Pending position structure that tracks current location and is updated with next candidate information

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - GinPageGetOpaque
  - [ReadBuffer](../R/ReadBuffer.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - GinPageHasFullRow
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
- Called from (representative examples):
  - [collectMatchesForHeapRow](../c/collectMatchesForHeapRow.md)
  - [scanPendingInsert](scanPendingInsert.md)

## Notes and Other Information
Essential for pending list processing during GIN index scans. The function implements proper buffer management to prevent race conditions with vacuum/cleanup processes. It handles both scenarios where heap rows are contained within single pages and where they span multiple pages, which can occur due to the way pending entries are inserted and organized.