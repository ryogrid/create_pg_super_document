# multirange_before_multirange_internal

## Location
[src/backend/utils/adt/multirangetypes.c:2424-2445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2424-L2445)

## Overview
Internal function that determines whether one multirange is strictly before (left of) another multirange by comparing the rightmost bound of the first with the leftmost bound of the second.

## Definition

```c
bool
multirange_before_multirange_internal(TypeCacheEntry *rangetyp,
									  const MultirangeType *mr1,
									  const MultirangeType *mr2)
```
## Detailed Description
This internal function implements the core logic for determining if one multirange is strictly before another multirange. It performs the comparison by extracting the upper bound of the rightmost range in the first multirange and the lower bound of the leftmost range in the second multirange, then checking if the upper bound is less than the lower bound.

The function handles empty multiranges by returning false, as empty sets cannot have meaningful ordering relationships. The algorithm ensures that all ranges in the first multirange come before all ranges in the second multirange.

## Parameters / Member Variables
- `*rangetyp`: TypeCacheEntry containing range type information and comparison functions
- `*mr1`: The first multirange to check (left operand)
- `*mr2`: The second multirange to compare against (right operand)
## Dependencies
- Functions called/Symbols referenced:
  - : Check if a multirange is empty
  - : Get bounds from a specific range within a multirange
  - : Compare two range bounds
  - : Structure representing range boundaries
  - : Multirange type structure with rangeCount member
- Called from (representative examples):
  - : Public function wrapper for "<<" operator
  - : Used in reverse for ">>" operator implementation

## Notes and Other Information
- Returns false for empty multiranges as they cannot have meaningful ordering relationships
- Compares the rightmost range of mr1 (at index ) with the leftmost range of mr2 (at index 0)
- The comparison is strict ("strictly before") rather than "before or overlapping"
- This function is the core implementation used by both "before" and "after" operators through argument swapping
- Assumes multiranges are properly normalized and sorted internally
- Critical for multirange ordering operations and can be used in indexing and sorting contexts

## Simplified Source

```c
bool
multirange_before_multirange_internal(TypeCacheEntry *rangetyp,
                                      const MultirangeType *multirange1,
                                      const MultirangeType *multirange2)
{
    RangeBound lower1, upper1, lower2, upper2;

    // Return false for empty inputs
    if (MultirangeIsEmpty(multirange1) || MultirangeIsEmpty(multirange2))
        return false;

    // Get bounds from the rightmost range of the first multirange
    multirange_get_bounds(rangetyp, multirange1, multirange1->rangeCount - 1,
                         &lower1, &upper1);

    // Get bounds from the leftmost range of the second multirange
    multirange_get_bounds(rangetyp, multirange2, 0,
                         &lower2, &upper2);

    // Check if first multirange's rightmost upper bound < second multirange's leftmost lower bound
    return (range_cmp_bounds(rangetyp, &upper1, &lower2) < 0);
}
```