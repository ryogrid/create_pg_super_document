# range_gist_penalty

## Location
[src/backend/utils/adt/rangetypes_gist.c:362-618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L362-L618)

## Overview
Implements the GiST penalty function for range types, calculating the cost of inserting a new range into an existing index entry to guide optimal page split decisions.

## Definition

```c
Datum
range_gist_penalty(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a crucial component of the GiST index implementation for range types. It calculates a penalty value that represents the cost of adding a new range to an existing index entry. This penalty guides the GiST algorithm in making optimal decisions about where to insert new entries and how to split index pages when they become full.

The function implements a sophisticated penalty calculation strategy with the following goals (in order of priority):
1. Keep normal ranges separate from empty and infinite ranges
2. Avoid broadening the class of the original predicate
3. Minimize broadening (as measured by subtype_diff) of the original predicate
4. Favor adding ranges to narrower original predicates

The penalty calculation varies based on the types of ranges being considered:
- Empty ranges: Handled specially with different penalties based on original range type
- Infinite ranges: (-inf, +inf), (-inf, x), or (x, +inf) with specific penalty calculations
- Normal ranges: Non-empty, finite ranges with extension-based penalty calculation

## Parameters / Member Variables
- : GiST entry representing the original index key (range)
- : GiST entry representing the new range to be inserted  
- : Output parameter that receives the calculated penalty value

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
- Called from (representative examples):
  - GiST index access method during index construction and maintenance

## Notes and Other Information
- Uses different penalty constants for different types of range broadening scenarios
- Leverages subtype_diff functions when available to provide precise penalty calculations based on the actual data type
- Falls back to default penalty constants when subtype_diff is unavailable
- Infinite penalties are assigned to prevent undesirable mixing of normal and infinite/empty ranges
- The penalty values only need to be meaningful within the same class of new ranges being inserted
- Critical for maintaining good index performance by ensuring similar ranges are clustered together
- Located in src/backend/utils/adt/rangetypes_gist.c:362-618

## Simplified Source

```c
Datum
range_gist_penalty(PG_FUNCTION_ARGS)
{
	GISTENTRY  *origentry = (GISTENTRY *) PG_GETARG_POINTER(0);
	GISTENTRY  *newentry = (GISTENTRY *) PG_GETARG_POINTER(1);
	float	   *penalty = (float *) PG_GETARG_POINTER(2);
	RangeType  *orig = DatumGetRangeTypeP(origentry->key);
	RangeType  *new = DatumGetRangeTypeP(newentry->key);
	TypeCacheEntry *typcache;
	bool		has_subtype_diff;
	RangeBound	orig_lower, new_lower, orig_upper, new_upper;
	bool		orig_empty, new_empty;

	typcache = range_get_typcache(fcinfo, RangeTypeGetOid(orig));
	has_subtype_diff = OidIsValid(typcache->rng_subdiff_finfo.fn_oid);

	// Extract bounds from both ranges
	range_deserialize(typcache, orig, &orig_lower, &orig_upper, &orig_empty);
	range_deserialize(typcache, new, &new_lower, &new_upper, &new_empty);

	// Handle empty range insertion
	if (new_empty) {
		if (orig_empty)
			*penalty = 0.0;  // Best case: empty to empty
		else if (RangeIsOrContainsEmpty(orig))
			*penalty = CONTAIN_EMPTY_PENALTY;
		else if (orig_lower.infinite && orig_upper.infinite)
			*penalty = 2 * CONTAIN_EMPTY_PENALTY;
		else if (orig_lower.infinite || orig_upper.infinite)
			*penalty = 3 * CONTAIN_EMPTY_PENALTY;
		else
			*penalty = 4 * CONTAIN_EMPTY_PENALTY;  // Worst case
	}
	// Handle infinite range insertion
	else if (new_lower.infinite && new_upper.infinite) {
		if (orig_lower.infinite && orig_upper.infinite)
			*penalty = 0.0;
		else if (orig_lower.infinite || orig_upper.infinite)
			*penalty = INFINITE_BOUND_PENALTY;
		else
			*penalty = 2 * INFINITE_BOUND_PENALTY;
		if (RangeIsOrContainsEmpty(orig))
			*penalty += CONTAIN_EMPTY_PENALTY;
	}
	// Handle normal range insertion
	else {
		if (orig_empty || orig_lower.infinite || orig_upper.infinite) {
			*penalty = get_float4_infinity();  // Avoid mixing classes
		} else {
			// Calculate extension penalty
			float8 diff = 0.0;
			if (range_cmp_bounds(typcache, &new_lower, &orig_lower) < 0) {
				if (has_subtype_diff)
					diff += call_subtype_diff(typcache, orig_lower.val, new_lower.val);
				else
					diff += DEFAULT_SUBTYPE_DIFF_PENALTY;
			}
			if (range_cmp_bounds(typcache, &new_upper, &orig_upper) > 0) {
				if (has_subtype_diff)
					diff += call_subtype_diff(typcache, new_upper.val, orig_upper.val);
				else
					diff += DEFAULT_SUBTYPE_DIFF_PENALTY;
			}
			*penalty = diff;
		}
	}

	PG_RETURN_POINTER(penalty);
}
```