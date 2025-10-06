# range_before_multirange_internal

## Location
[src/backend/utils/adt/multirangetypes.c:2402-2423](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2402-L2423)

## Overview
Internal function that determines whether a single range is strictly before (left of) a multirange by comparing range bounds.

## Definition

```c
bool
range_before_multirange_internal(TypeCacheEntry *rangetyp,
								 const RangeType *r,
								 const MultirangeType *mr)
```
## Detailed Description
This internal function implements the core logic for determining if a range is strictly before a multirange. It performs the comparison by checking if the upper bound of the range is less than the lower bound of the multirange. The function handles empty ranges and multiranges by returning false, as empty sets cannot have a meaningful "before" relationship.

The function deserializes the range bounds and gets the bounds of the multirange's first range (index 0), then uses  to perform the actual comparison.

## Parameters / Member Variables
- `*rangetyp`: TypeCacheEntry containing range type information and comparison functions
- `*r`: The range to check (left operand)
- `*mr`: The multirange to compare against (right operand)
## Dependencies
- Functions called/Symbols referenced:
  - : Check if a range is empty
  - : Check if a multirange is empty
  - : Extract bounds from a range
  - : Get bounds from a specific range within a multirange
  - : Compare two range bounds
  - : Structure representing range boundaries
  - : Debugging assertion macro
- Called from (representative examples):
  - : Public function wrapper
  - : Inverse operation implementation
  - : GiST index consistency checking
  - : GiST leaf node consistency checking

## Notes and Other Information
- Returns false for empty ranges or multiranges as they cannot have meaningful ordering relationships
- Only compares against the first range (index 0) of the multirange, which represents the leftmost boundary
- The function assumes the multirange is properly sorted and normalized
- Used internally by both public operator functions and GiST indexing operations
- The comparison is strict ("strictly left of") rather than "left of or overlapping"

## Simplified Source

```c
bool
range_before_multirange_internal(TypeCacheEntry *rangetyp,
                                 const RangeType *range,
                                 const MultirangeType *multirange)
{
    RangeBound lower1, upper1, lower2, upper2;
    bool empty;

    // Return false for empty inputs
    if (RangeIsEmpty(range) || MultirangeIsEmpty(multirange))
        return false;

    // Extract bounds from the range
    range_deserialize(rangetyp, range, &lower1, &upper1, &empty);
    Assert(!empty);

    // Get bounds from the first range in the multirange
    multirange_get_bounds(rangetyp, multirange, 0, &lower2, &upper2);

    // Check if range's upper bound < multirange's lower bound
    return (range_cmp_bounds(rangetyp, &upper1, &lower2) < 0);
}
```