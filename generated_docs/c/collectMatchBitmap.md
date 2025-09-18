# collectMatchBitmap

## Location
src/backend/access/gin/ginget.c: 121 - 318

## Overview
This is a central function in GIN index scanning that collects tuple identifiers (TIDs) into a match bitmap for all heap tuples that satisfy the search criteria, supporting multiple scan modes including partial matching, ALL mode, and EVERYTHING mode.

## Definition
```c
static bool collectMatchBitmap(GinBtreeData *btree, GinBtreeStack *stack, GinScanEntry scanEntry, Snapshot snapshot)
```

## Detailed Description
The function implements the core logic for collecting matching TIDs from a GIN index entry tree. It supports three distinct search modes:

1. **Partial-match support**: Scans from the current position until the comparePartialFn indicates completion
2. **SEARCH_MODE_ALL**: Scans from the current position until hitting null items or end of attribute
3. **SEARCH_MODE_EVERYTHING**: Scans from the current position until end of attribute

The function handles both posting lists (stored directly in index tuples) and posting trees (separate B-tree structures for large posting lists). For posting trees, it temporarily unlocks pages to prevent deadlocks with vacuum processes and re-finds the position after scanning. The function maintains predicate locks for proper isolation and updates result count predictions for query optimization.

## Parameters / Member Variables
- `btree`: Pointer to GinBtreeData containing btree state and ginstate information
- `stack`: Pointer to GinBtreeStack representing current scan position in the btree
- `scanEntry`: Pointer to GinScanEntry containing search criteria and result bitmap
- `snapshot`: Snapshot for MVCC consistency and predicate locking

## Dependencies
- Functions called/Symbols referenced:
  - [tbm_create](../t/tbm_create.md) (creates the match bitmap)
  - [moveRightIfItNeeded](../m/moveRightIfItNeeded.md) (page navigation helper)
  - [gintuple_get_attrnum](../g/gintuple_get_attrnum.md), gintuple_get_key (tuple access functions)
  - [scanPostingTree](../s/scanPostingTree.md) (for scanning posting trees)
  - [ginReadTuple](../g/ginReadTuple.md), tbm_add_tuples (for direct posting list processing)
  - [ginCompareEntries](../g/ginCompareEntries.md) (for key comparison during re-find)
  - [datumCopy](../d/datumCopy.md) (for value copying during tree scans)
  - [PredicateLockPage](../P/PredicateLockPage.md) (for predicate locking)
  - Various GIN constants (GIN_CAT_NORM_KEY, GIN_SEARCH_MODE_ALL, etc.)
- Called from:
  - [startScanEntry](../s/startScanEntry.md) (src/backend/access/gin/ginget.c:365)

## Notes and Other Information
- This is a static function, only accessible within the ginget.c file
- Returns `true` when scan is complete, `false` if restart from scratch is necessary
- Handles complex locking scenarios to prevent deadlocks with concurrent vacuum operations
- Implements sophisticated re-find logic after unlocking pages during posting tree scans
- Critical component of GIN index query execution that enables efficient bitmap-based result collection
- Supports both exact and partial matching strategies
- Manages memory allocation for copied datums when scanning posting trees
- Maintains scan position across page boundaries and posting tree traversals