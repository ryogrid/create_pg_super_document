# multirange_bsearch_match

## Location
src/backend/utils/adt/multirangetypes.c: 898 - 940

## Overview
Performs binary search within a multirange to find if any range matches a given key using a custom comparison function.

## Definition


## Detailed Description
This function implements a generic binary search algorithm for multirange types that can be customized with different comparison functions. It searches through the sorted ranges within a multirange to find one that matches the given key according to the provided comparison function. The comparison function determines both the search direction and whether a found range constitutes a match. This design allows the same binary search logic to be used for different operations like containment checks, overlap detection, and exact matching.

## Parameters / Member Variables
- : Type cache entry containing metadata and functions for the range element type
- : The multirange to search within
- : Pointer to the search key (type depends on the specific operation)
- : Comparison function that guides the search and determines matches

## Dependencies
- Functions called/Symbols referenced:
  - [multirange_get_bounds](multirange_get_bounds.md) (extracts range boundaries for comparison)
  - MultirangeType (multirange structure type)
  - RangeBound (boundary structure type)
- Called from (representative examples):
  - [multirange_contains_elem_internal](multirange_contains_elem_internal.md)
  - [multirange_contains_range_internal](multirange_contains_range_internal.md)
  - [range_overlaps_multirange_internal](../r/range_overlaps_multirange_internal.md)

## Notes and Other Information
- This is a static function, internal to the multirange implementation
- Uses standard binary search algorithm with O(log n) complexity
- The comparison function can report both search direction and match status
- Supports various operations through different comparison functions
- Returns false if no range is found or if the comparison function reports no match
- Critical optimization for multirange operations that need to locate specific ranges
- Located in 