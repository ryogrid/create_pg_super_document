# cmp_orderbyvals

## Location
src/backend/executor/nodeIndexscan.c: 405 - 440

## Overview
The cmp_orderbyvals function compares ORDER BY expression values between two tuples, implementing proper null handling and using SortSupport for efficient comparisons.

## Definition
```c
static int cmp_orderbyvals(const Datum *adist, const bool *anulls,
                          const Datum *bdist, const bool *bnulls,
                          IndexScanState *node)
```

## Detailed Description
cmp_orderbyvals is a comparison function that determines the relative ordering between two sets of ORDER BY expression values. This function is essential for maintaining proper tuple ordering in index scans with ORDER BY clauses:

1. **Multi-Column Comparison**: Iterates through all ORDER BY keys in sequence, comparing corresponding values
2. **Null Handling**: Implements NULLS LAST semantics, where:
   - NULL values are considered greater than any non-NULL value
   - Two NULL values are considered equal
   - Only NULLS LAST ordering is supported (as determined by PostgreSQL's path matching logic)
3. **SortSupport Integration**: Uses the pre-configured SortSupport comparator functions for efficient, type-specific comparisons
4. **Early Termination**: Returns immediately when a difference is found, implementing lexicographic ordering for multi-column ORDER BY clauses
5. **Equality Handling**: Returns 0 when all ORDER BY values are equal between the two tuples

The function provides the foundation for tuple reordering decisions in IndexNextWithReorder and priority queue management.

## Parameters / Member Variables
- `adist`: Array of Datum values for the first tuple's ORDER BY expressions
- `anulls`: Array of boolean flags indicating NULL values for the first tuple
- `bdist`: Array of Datum values for the second tuple's ORDER BY expressions  
- `bnulls`: Array of boolean flags indicating NULL values for the second tuple
- `node`: IndexScanState containing the SortSupport structures and number of ORDER BY keys

## Dependencies
- Functions called/Symbols referenced:
  - SortSupport comparator functions (accessed via ssup->comparator)
- Called from (representative examples):
  - ReorderTuple (nodeIndexscan.c:62)
  - [IndexNextWithReorder](../I/IndexNextWithReorder.md) (nodeIndexscan.c:236, 303, 332)
  - [reorderqueue_cmp](../r/reorderqueue_cmp.md) (nodeIndexscan.c:449)

## Notes and Other Information
- This is a static function used internally within the index scan executor
- Returns standard comparison result: negative for less than, 0 for equal, positive for greater than
- Only supports NULLS LAST ordering due to PostgreSQL's index path matching constraints
- Leverages SortSupport infrastructure for optimized, type-specific comparisons
- Critical for correct tuple ordering in lossy index access methods
- Used both for immediate ordering decisions and for maintaining the reorder queue's priority ordering
- The function assumes that both input arrays have the same length (node->iss_NumOrderByKeys)
- SortSupport comparators are pre-initialized during index scan setup for optimal performance