# range_overright_multirange_internal

## Location
[src/backend/utils/adt/multirangetypes.c:2158-2178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2158-L2178)

## Overview
Internal function that checks if a range does not extend to the left of a multirange ("&>" operator implementation).

## Definition

```c
bool
range_overright_multirange_internal(TypeCacheEntry *rangetyp,
									const RangeType *r,
									const MultirangeType *mr)
```
## Detailed Description
This internal function implements the core logic for the "overright" or "does not extend to left of" operator (&>) between a range type and a multirange type. It determines whether the given range does not extend to the left of the given multirange by comparing their lower bounds.

The function extracts the bounds from the range and from the first range in the multirange (which represents the leftmost range), then compares their lower bounds. It returns true if the range's lower bound is greater than or equal to the multirange's lower bound, meaning the range does not extend beyond the leftmost point of the multirange.

## Parameters / Member Variables
- `*rangetyp`: TypeCacheEntry pointer providing type information for range operations
- `*r`: const RangeType pointer - The range to compare
- `*mr`: const MultirangeType pointer - The multirange to compare against
## Dependencies
- Functions called/Symbols referenced:
  -  - Check if range is empty
  -  - Check if multirange is empty
  -  - Extract bounds from range
  -  - Extract bounds from first range in multirange (index 0)
  -  - Compare range bounds
  -  - Debug assertion macro
- Called from (representative examples):
  -  - Public wrapper function
  -  - GiST index consistency check
  -  - GiST leaf consistency check
  - Referenced in  macro

## Notes and Other Information
- This is an internal function used by the public  function and GiST indexing operations
- Returns false if either the range or multirange is empty
- Uses the first range in the multirange (at index 0) to get the leftmost bounds for comparison
- The function comment indicates "does not extend to left of?" which corresponds to the &> operator
- Part of PostgreSQL's range and multirange type system for spatial/temporal comparisons
- Located in 
- The comparison returns true when the range's lower bound is greater than or equal to the multirange's lower bound

## Simplified Source

```c
bool range_overright_multirange_internal(TypeCacheEntry *rangetyp,
                                        const RangeType *r,
                                        const MultirangeType *mr) {
    // Return false if either is empty
    if (RangeIsEmpty(r) || MultirangeIsEmpty(mr))
        return false;

    // Get bounds from range and leftmost range in multirange
    RangeBound r_lower, r_upper, mr_lower, mr_upper;
    bool empty;

    range_deserialize(rangetyp, r, &r_lower, &r_upper, &empty);
    multirange_get_bounds(rangetyp, mr, 0, &mr_lower, &mr_upper);

    // Range overright multirange if range's lower >= multirange's lower
    return (range_cmp_bounds(rangetyp, &r_lower, &mr_lower) >= 0);
}
```