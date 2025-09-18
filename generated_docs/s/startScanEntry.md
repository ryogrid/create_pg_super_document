# startScanEntry

## Location
[src/backend/access/gin/ginget.c:319-487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginget.c#L319-L487)

## Overview
This is a setup function that initializes the beginning state of GIN index scans by locating the correct buffer, pinning it, and preparing the scan entry for either posting tree traversal or in-memory posting list processing.

## Definition
```c
static void startScanEntry(GinState *ginstate, GinScanEntry entry, Snapshot snapshot)
```

## Detailed Description
The function performs comprehensive initialization for a GIN scan entry, handling different types of search scenarios:

1. **Partial Match/Empty Query Mode**: Uses `collectMatchBitmap` to gather all matching TIDs into a bitmap for complex matching scenarios
2. **Exact Match with Posting Tree**: For large posting lists stored as separate B-trees, initializes posting tree scanning by loading the first leaf page
3. **Exact Match with Posting List**: For small posting lists stored directly in index tuples, reads the list into memory
4. **No Match Found**: Handles cases where no matching entry exists

The function includes sophisticated error handling and restart logic for cases where the GIN tree structure changes during scanning (due to concurrent operations). It manages buffer locking carefully to prevent deadlocks with vacuum processes and applies appropriate predicate locks for isolation guarantees.

## Parameters / Member Variables
- `ginstate`: Pointer to GinState containing index metadata and operator information
- `entry`: Pointer to GinScanEntry structure to be initialized with scan state
- `snapshot`: Snapshot for MVCC consistency and predicate locking

## Dependencies
- Functions called/Symbols referenced:
  - [ginPrepareEntryScan](../g/ginPrepareEntryScan.md) (prepares btree entry for search)
  - [ginFindLeafPage](../g/ginFindLeafPage.md) (locates the appropriate leaf page)
  - [collectMatchBitmap](../c/collectMatchBitmap.md) (collects TIDs for partial/complex matches)
  - [ginScanBeginPostingTree](../g/ginScanBeginPostingTree.md) (initializes posting tree scanning)
  - [GinDataLeafPageGetItems](../G/GinDataLeafPageGetItems.md) (extracts items from posting tree page)
  - [ginReadTuple](../g/ginReadTuple.md) (reads posting list from index tuple)
  - [tbm_begin_iterate](../t/tbm_begin_iterate.md), tbm_end_iterate, tbm_free, tbm_is_empty (bitmap operations)
  - [PredicateLockPage](../P/PredicateLockPage.md) (applies predicate locks)
  - [IncrBufferRefCount](../I/IncrBufferRefCount.md) (manages buffer reference counting)
  - Various GIN utility functions and constants
- Called from:
  - [startScan](startScan.md) (src/backend/access/gin/ginget.c:610)

## Notes and Other Information
- This is a static function, only accessible within the ginget.c file
- Contains a restart mechanism (`restartScanEntry` label) for handling concurrent tree restructuring
- Manages complex buffer locking scenarios to prevent deadlocks with maintenance operations
- Handles three distinct scan scenarios: bitmap collection, posting tree scanning, and direct posting list reading
- Initializes multiple scan entry fields including matchBitmap, matchIterator, list, nlist, and prediction counters
- Critical component of GIN index scan initialization that sets up the appropriate data structures for efficient query execution
- Implements proper cleanup and resource management for error scenarios
- Applies granular predicate locking strategies depending on the type of scan being performed