# range_contains_internal

## Location
[src/backend/utils/adt/rangetypes.c:2586-2617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L2586-L2617)

## Overview
Tests whether one range contains another range by comparing their bounds to determine if the first range completely encompasses the second range.

## Definition
```c
bool range_contains_internal(TypeCacheEntry *typcache, const RangeType *r1, const RangeType *r2)
```

## Detailed Description
The `range_contains_internal` function implements the core logic for range containment testing. It deserializes both input ranges and compares their bounds to determine if range r1 completely contains range r2. The containment logic handles empty ranges as special cases and uses bound comparison functions to ensure that r1's lower bound is less than or equal to r2's lower bound, and r1's upper bound is greater than or equal to r2's upper bound.

The function assumes that both ranges are of the same type (verified by the caller) and uses the type cache for efficient bound comparisons. Empty ranges are handled according to standard mathematical set theory: any range contains an empty range, but an empty range cannot contain a non-empty range.

## Parameters / Member Variables
- `typcache`: Type cache entry containing comparison functions and type information for the range element type
- `r1`: The potentially containing range (left operand)
- `r2`: The potentially contained range (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - RangeBound (struct type for representing range bounds)
  - RangeTypeGetOid (function to get range type OID for verification)
  - [range_deserialize](range_deserialize.md) (function to extract bounds and empty flag from range)
  - [range_cmp_bounds](range_cmp_bounds.md) (function to compare range bounds using type-specific comparison)
  - elog (PostgreSQL error logging function)
- Called from (representative examples):
  - [range_contains](range_contains.md) (public SQL-callable function)
  - [range_contained_by_internal](range_contained_by_internal.md) (used for symmetric containment test)
  - [range_gist_consistent_int_range](range_gist_consistent_int_range.md) (GiST index support)
  - [range_gist_consistent_leaf_range](range_gist_consistent_leaf_range.md) (GiST index support)
  - [spg_range_quad_leaf_consistent](../s/spg_range_quad_leaf_consistent.md) (SP-GiST index support)

## Notes and Other Information
- This is an internal function used by various range operators and index support functions
- Returns true if r1 contains r2, false otherwise
- Handles edge cases: empty r2 is contained by any r1, empty r1 contains no non-empty r2
- Uses type-safe comparison through the typcache mechanism
- Critical for implementing the @> (contains) and <@ (contained by) operators
- Used extensively in range indexing strategies for query optimization
- Assumes caller has verified that both ranges are of compatible types

## Simplified Source

```c
bool range_contains_internal(TypeCacheEntry *typcache, const RangeType *r1, const RangeType *r2) {
    RangeBound lower1, upper1, lower2, upper2;
    bool empty1, empty2;

    // Verify same range type
    if (RangeTypeGetOid(r1) != RangeTypeGetOid(r2))
        elog(ERROR, "range types do not match");

    // Extract bounds from both ranges
    range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
    range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

    // Handle empty range cases
    if (empty2)
        return true;   // Any range contains empty range
    else if (empty1)
        return false;  // Empty range contains no non-empty range

    // For containment: r1.lower <= r2.lower AND r1.upper >= r2.upper
    if (range_cmp_bounds(typcache, &lower1, &lower2) > 0)
        return false;  // r1 lower bound is greater than r2 lower bound
    if (range_cmp_bounds(typcache, &upper1, &upper2) < 0)
        return false;  // r1 upper bound is less than r2 upper bound

    return true;
}
```