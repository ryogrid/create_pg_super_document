# multirange_contains_range_internal

## Location
src/backend/utils/adt/multirangetypes.c: 1801 - 1828

## Overview
An internal function that tests whether a multirange contains a specific range using efficient binary search.

## Definition
```c
bool multirange_contains_range_internal(TypeCacheEntry *rangetyp,
                                       const MultirangeType *mr,
                                       const RangeType *r)
```

## Detailed Description
This function determines if a multirange completely contains a given range. It implements the core logic for multirange-to-range containment operations. The function handles edge cases efficiently: empty ranges are considered to be contained by any multirange (including empty ones), while empty multiranges contain no non-empty ranges. For non-empty cases, it deserializes the range bounds and uses binary search with a specialized comparison function to locate a containing range within the multirange.

## Parameters / Member Variables
- `rangetyp`: TypeCacheEntry pointer containing type-specific information for range operations
- `mr`: const MultirangeType pointer to the multirange that may contain the range
- `r`: const RangeType pointer to the range being tested for containment

## Dependencies
- Functions called/Symbols referenced:
  - `RangeIsEmpty` - Check if a range is empty
  - `MultirangeIsEmpty` - Check if a multirange is empty
  - `range_deserialize` - Extract bounds from range structure
  - `multirange_bsearch_match` - Perform binary search with custom comparison
  - `multirange_range_contains_bsearch_comparison` - Comparison function for binary search
  - `RangeBound` - Structure for representing range boundaries
- Called from (representative examples):
  - `multirange_contains_range` - Public SQL function wrapper
  - `range_contained_by_multirange` - Inverse containment operation
  - `range_gist_consistent_leaf_multirange` - GiST index consistency checking

## Notes and Other Information
- Handles empty ranges as a special case - they are considered contained by any multirange
- Uses binary search for efficient O(log n) lookup in sorted multirange structure
- The function leverages the property that multiranges maintain non-overlapping, sorted ranges
- Returns boolean result suitable for SQL boolean operations
- Part of PostgreSQL's range type system for handling complex range operations efficiently