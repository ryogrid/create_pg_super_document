# range_adjacent_multirange_internal

## Location
[src/backend/utils/adt/multirangetypes.c:2471-2506](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2471-L2506)

## Overview
Internal function that determines whether a range is adjacent to any part of a multirange by checking if the range touches either the leftmost or rightmost boundary of the multirange.

## Definition

```c
bool
range_adjacent_multirange_internal(TypeCacheEntry *rangetyp,
								   const RangeType *r,
								   const MultirangeType *mr)
```
## Detailed Description
This internal function implements the core logic for determining if a range is adjacent to a multirange. It performs two main checks: first, it tests if the range's upper bound is adjacent to the multirange's leftmost lower bound. If the multirange contains multiple ranges, it also checks if the range's lower bound is adjacent to the multirange's rightmost upper bound.

The function handles empty ranges and multiranges by returning false, as empty sets cannot have meaningful adjacency relationships. The algorithm uses the  function to determine if two range bounds are touching without overlapping.

## Parameters / Member Variables
- `*rangetyp`: TypeCacheEntry containing range type information and comparison functions
- `*r`: The range to check for adjacency
- `*mr`: The multirange to compare against
## Dependencies
- Functions called/Symbols referenced:
  - : Check if a range is empty
  - : Check if a multirange is empty
  - : Extract bounds from a range
  - : Get bounds from a specific range within a multirange
  - : Check if two range bounds are adjacent (touching)
  - : Structure representing range boundaries
  - : Debugging assertion macro
  - : Integer type for range count
- Called from (representative examples):
  - : Public function wrapper for "-|-" operator
  - : Used in reverse for multirange-to-range adjacency
  - : GiST index consistency checking
  - : GiST leaf node consistency checking

## Notes and Other Information
- Returns false for empty ranges or multiranges as they cannot have meaningful adjacency relationships
- Performs up to two adjacency checks: one against the leftmost bound and optionally one against the rightmost bound
- Only checks the rightmost bound if the multirange contains more than one range ()
- The adjacency check is symmetric - it works regardless of which side of the multirange the range is on
- Uses  which checks for touching boundaries without overlap
- Critical for range operations that need to detect when ranges can be merged or are touching
- Optimized to avoid unnecessary bound extraction when multirange has only one range