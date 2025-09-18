# scanPendingInsert

## Location
[src/backend/access/gin/ginget.c:1824-1914](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginget.c#L1824-L1914)

## Overview
Scans the entire GIN pending list to collect all matching heap row TIDs into a bitmap, coordinating between pending list processing and consistent function evaluation.

## Definition
```c
static void scanPendingInsert(IndexScanDesc scan, TIDBitmap *tbm, int64 *ntids)
```

## Detailed Description
This function implements the complete pending list scan workflow for GIN bitmap index scans. It starts by acquiring the pending list head from the metapage, then iterates through all heap rows in the pending list using scanGetCandidate. For each row, it calls collectMatchesForHeapRow to determine entry matches, then applies the boolean consistent functions to make final match decisions.

The function properly handles predicate locking on the metapage to coordinate with fast-update insertions, and manages memory contexts to control allocation during consistent function calls. Successfully matching rows are added to the provided TID bitmap with appropriate recheck flags.

## Parameters / Member Variables
- `scan`: Index scan descriptor containing scan configuration and state
- `tbm`: TID bitmap to collect matching tuple identifiers
- `ntids`: Output parameter to count the number of matching TIDs found

## Dependencies
- Functions called/Symbols referenced:
  - [ReadBuffer](../R/ReadBuffer.md)
  - [PredicateLockPage](../P/PredicateLockPage.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - GinPageGetMeta
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [scanGetCandidate](scanGetCandidate.md)
  - [collectMatchesForHeapRow](../c/collectMatchesForHeapRow.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [tbm_add_tuples](../t/tbm_add_tuples.md)
- Called from (representative examples):
  - [gingetbitmap](../g/gingetbitmap.md)

## Notes and Other Information
Key entry point for pending list processing in GIN bitmap scans. The predicate locking ensures proper concurrency control with fast-update operations. Memory context management is essential to prevent memory leaks during consistent function evaluation, as these functions may allocate significant temporary memory. The function gracefully handles empty pending lists by returning immediately.