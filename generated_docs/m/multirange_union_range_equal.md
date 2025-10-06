# multirange_union_range_equal

## Location
[src/backend/utils/adt/rangetypes_gist.c:888-914](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L888-L914)

## Overview
A static helper function that determines whether the union of a multirange equals a given single range by comparing their overall bounds.

## Definition

```c
static bool
multirange_union_range_equal(TypeCacheEntry *typcache,
							 const RangeType *r,
							 const MultirangeType *mr)
```
## Detailed Description
This function performs an efficient equality check between a single range and the union of a multirange without actually computing the union. It leverages the fact that if a multirange's union equals a single range, then:

1. The overall lower bound of the multirange must equal the single range's lower bound
2. The overall upper bound of the multirange must equal the single range's upper bound

The function handles special cases for empty ranges and multiranges appropriately. For non-empty inputs, it extracts the bounds from the first and last range in the multirange (which represent the overall bounds when ranges are properly ordered) and compares them with the single range's bounds.

This optimization is particularly useful in GiST index operations where we need to determine if a multirange can be represented by a single equivalent range without the computational overhead of actually performing the union.

## Parameters / Member Variables
- : TypeCacheEntry providing type information and comparison functions for the range element type
- : Single RangeType to compare against the multirange union
- : MultirangeType whose union should be compared with the single range
- Returns: boolean indicating whether the multirange union equals the given range

## Dependencies
- Functions called/Symbols referenced:
  - : Check if a single range is empty
  - : Check if a multirange is empty
  - : Extract bounds from a single range
  - : Extract bounds from specific range in multirange
  - : Compare range bounds for equality
- Called from (representative examples):
  - : GiST consistency checking for multirange leaf entries

## Notes and Other Information
- Located in src/backend/utils/adt/rangetypes_gist.c:888-914
- Static function used internally within GiST range operations
- Provides an optimization by avoiding actual multirange union computation
- Assumes multirange ranges are properly ordered (first has overall lower bound, last has overall upper bound)
- Essential for efficient GiST index operations involving multiranges
- Handles empty range cases correctly by ensuring both inputs have the same emptiness status

## Simplified Source

```c
static bool
multirange_union_range_equal(TypeCacheEntry *typcache,
							 const RangeType *range,
							 const MultirangeType *multirange)
{
	RangeBound	range_lower, range_upper;
	RangeBound	mr_lower, mr_upper, temp;
	bool		empty;

	// Handle empty cases
	if (RangeIsEmpty(range) || MultirangeIsEmpty(multirange))
		return (RangeIsEmpty(range) && MultirangeIsEmpty(multirange));

	// Extract bounds from single range
	range_deserialize(typcache, range, &range_lower, &range_upper, &empty);

	// Extract overall bounds from multirange (first and last ranges)
	multirange_get_bounds(typcache, multirange, 0, &mr_lower, &temp);
	multirange_get_bounds(typcache, multirange, multirange->rangeCount - 1, &temp, &mr_upper);

	// Compare overall bounds
	return (range_cmp_bounds(typcache, &range_lower, &mr_lower) == 0 &&
			range_cmp_bounds(typcache, &range_upper, &mr_upper) == 0);
}
```