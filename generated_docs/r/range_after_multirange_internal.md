# range_after_multirange_internal

## Location
[src/backend/utils/adt/multirangetypes.c:2446-2470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2446-L2470)

## Overview
Internal function that determines whether a single range is strictly after (right of) a multirange by comparing the range's lower bound with the multirange's rightmost upper bound.

## Definition

```c
bool
range_after_multirange_internal(TypeCacheEntry *rangetyp,
								const RangeType *r,
								const MultirangeType *mr)
```
## Detailed Description
This internal function implements the core logic for determining if a range is strictly after a multirange. It performs the comparison by checking if the lower bound of the range is greater than the upper bound of the rightmost range in the multirange. The function handles empty ranges and multiranges by returning false, as empty sets cannot have meaningful ordering relationships.

The function deserializes the range bounds and gets the bounds of the multirange's rightmost range (at index ), then uses  to perform the actual comparison with a "greater than" check.

## Parameters / Member Variables
- : TypeCacheEntry containing range type information and comparison functions
- : The range to check (left operand)
- : The multirange to compare against (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - : Check if a range is empty
  - : Check if a multirange is empty
  - : Extract bounds from a range
  - : Get bounds from a specific range within a multirange
  - : Compare two range bounds
  - : Structure representing range boundaries
  - : Debugging assertion macro
  - : Integer type for range count
- Called from (representative examples):
  - : Public function wrapper for ">>" operator
  - : Used in reverse for "<<" operator implementation
  - : GiST index consistency checking
  - : GiST leaf node consistency checking

## Notes and Other Information
- Returns false for empty ranges or multiranges as they cannot have meaningful ordering relationships
- Compares against the rightmost range (at index ) of the multirange, which represents the rightmost boundary
- The function assumes the multirange is properly sorted and normalized
- Used internally by both public operator functions and GiST indexing operations
- The comparison is strict ("strictly right of") rather than "right of or overlapping"
- Complements  by checking the opposite ordering relationship