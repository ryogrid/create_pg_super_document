# range_gist_consistent_leaf_range

## Location
src/backend/utils/adt/rangetypes_gist.c: 1058 - 1092

## Overview
Performs GiST consistent test on an index leaf page with range query, determining whether a stored range matches the query criteria using the specified spatial relationship strategy.

## Definition
```c
static bool range_gist_consistent_leaf_range(TypeCacheEntry *typcache,
                                             StrategyNumber strategy,
                                             const RangeType *key,
                                             const RangeType *query)
```

## Detailed Description
This function implements the consistent test for GiST index operations on leaf pages when both the stored data and the query are range types. Unlike internal node consistency tests that need to be conservative, leaf page tests can be exact since they're testing the actual stored data rather than approximating subtree contents.

The function supports all standard range spatial relationship operators and directly delegates to the appropriate range comparison function based on the strategy. This provides exact matching semantics for all range-to-range comparisons including positional relationships (before, after, overlapping), containment relationships (contains, contained by), adjacency, and equality.

## Parameters / Member Variables
- `typcache`: Type cache entry containing information about the range type being indexed
- `strategy`: Strategy number indicating the type of spatial relationship to test (e.g., RANGESTRAT_OVERLAPS, RANGESTRAT_CONTAINS)
- `key`: The range value stored in this leaf index entry
- `query`: The range value being searched for in the index

## Dependencies
- Functions called/Symbols referenced:
  - [range_before_internal](range_before_internal.md)
  - [range_overleft_internal](range_overleft_internal.md)
  - [range_overlaps_internal](range_overlaps_internal.md)
  - [range_overright_internal](range_overright_internal.md)
  - [range_after_internal](range_after_internal.md)
  - [range_adjacent_internal](range_adjacent_internal.md)
  - [range_contains_internal](range_contains_internal.md)
  - [range_contained_by_internal](range_contained_by_internal.md)
  - [range_eq_internal](range_eq_internal.md)
  - RANGESTRAT_* constants
  - elog (for error handling)
- Called from (representative examples):
  - rangeCopy
  - [range_gist_consistent](range_gist_consistent.md)
  - [multirange_gist_consistent](../m/multirange_gist_consistent.md)

## Notes and Other Information
- This is a static function used internally within the range types GiST implementation
- Unlike internal node consistency functions, this provides exact matching since it operates on actual stored data
- Supports all nine standard range strategy operators for comprehensive spatial relationship testing
- The function acts as a strategy dispatcher, delegating to specialized internal comparison functions
- Used specifically for leaf-level index operations where precision is required rather than the conservative approximations needed for internal nodes
- Each strategy maps directly to a corresponding range comparison function that implements the exact semantics of that spatial relationship