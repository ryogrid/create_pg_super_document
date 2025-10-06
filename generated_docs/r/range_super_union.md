# range_super_union

## Location
[src/backend/utils/adt/rangetypes_gist.c:821-887](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L821-L887)

## Overview
A specialized range union function for GiST indexes that computes the smallest range containing two input ranges while tracking empty range information for indexing optimization.

## Definition

```c
static RangeType *
range_super_union(TypeCacheEntry *typcache, RangeType *r1, RangeType *r2)
```
## Detailed Description
This static function is a critical component of the GiST range indexing infrastructure. It differs from the regular range_union function in two important ways:

1. **Non-adjacent tolerance**: Unlike regular range union which throws errors for non-adjacent ranges, this function absorbs intervening values into the result range, making it suitable for index node bounding boxes.

2. **Empty range tracking**: It meticulously tracks whether any empty range has been incorporated into the result using the RANGE_CONTAIN_EMPTY flag. This enables efficient indexed searches for contained_by operations.

The function implements an optimized algorithm:
- Handles empty range cases first by preserving or setting the RANGE_CONTAIN_EMPTY flag
- For non-empty ranges, selects the minimum lower bound and maximum upper bound
- Includes optimization to avoid constructing new ranges when one input already represents the result
- Ensures all GiST union operations preserve empty range semantics

## Parameters / Member Variables
- : TypeCacheEntry for the range's element type, providing comparison and type information
- : First RangeType input to union
- : Second RangeType input to union
- Returns: RangeType pointer representing the smallest range containing both inputs

## Dependencies
- Functions called/Symbols referenced:
  - : Extract bounds and empty status from range
  - : Get flag bits from range
  - : Create a copy of a range
  - : Set the RANGE_CONTAIN_EMPTY flag
  - : Compare range bounds
  - : Construct new range from bounds
- Called from (representative examples):
  - : GiST union method for range types
  - Other GiST internal operations

## Notes and Other Information
- Located in src/backend/utils/adt/rangetypes_gist.c:821-887
- Static function used exclusively within the GiST range implementation
- Essential for maintaining correct bounding box semantics in GiST range indexes
- The empty range tracking is crucial for supporting contained_by (@>) operator indexing
- Implements important optimizations to avoid unnecessary range construction
- All GiST union operations for ranges must go through this function to maintain consistency

## Simplified Source

```c
static RangeType *
range_super_union(TypeCacheEntry *typcache, RangeType *r1, RangeType *r2)
{
	RangeType  *result;
	RangeBound	lower1, upper1, lower2, upper2;
	bool		empty1, empty2;
	char		flags1, flags2;
	RangeBound *result_lower, *result_upper;

	// Extract bounds and flags from both ranges
	range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
	range_deserialize(typcache, r2, &lower2, &upper2, &empty2);
	flags1 = range_get_flags(r1);
	flags2 = range_get_flags(r2);

	// Handle empty range cases
	if (empty1) {
		if (flags2 & (RANGE_EMPTY | RANGE_CONTAIN_EMPTY))
			return r2;  // r2 already handles empty
		r2 = rangeCopy(r2);
		range_set_contain_empty(r2);
		return r2;
	}
	if (empty2) {
		if (flags1 & (RANGE_EMPTY | RANGE_CONTAIN_EMPTY))
			return r1;  // r1 already handles empty
		r1 = rangeCopy(r1);
		range_set_contain_empty(r1);
		return r1;
	}

	// Find minimum lower bound and maximum upper bound
	result_lower = (range_cmp_bounds(typcache, &lower1, &lower2) <= 0) ? &lower1 : &lower2;
	result_upper = (range_cmp_bounds(typcache, &upper1, &upper2) >= 0) ? &upper1 : &upper2;

	// Optimization: avoid creating new range if one input is already correct
	if (result_lower == &lower1 && result_upper == &upper1 &&
		((flags1 & RANGE_CONTAIN_EMPTY) || !(flags2 & RANGE_CONTAIN_EMPTY)))
		return r1;
	if (result_lower == &lower2 && result_upper == &upper2 &&
		((flags2 & RANGE_CONTAIN_EMPTY) || !(flags1 & RANGE_CONTAIN_EMPTY)))
		return r2;

	// Create new union range
	result = make_range(typcache, result_lower, result_upper, false, NULL);

	// Preserve empty range tracking
	if ((flags1 & RANGE_CONTAIN_EMPTY) || (flags2 & RANGE_CONTAIN_EMPTY))
		range_set_contain_empty(result);

	return result;
}
```