# range_contains_internal

## Location
src/backend/utils/adt/rangetypes.c: 2586 - 2617

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
  - range_deserialize (function to extract bounds and empty flag from range)
  - range_cmp_bounds (function to compare range bounds using type-specific comparison)
  - elog (PostgreSQL error logging function)
- Called from (representative examples):
  - range_contains (public SQL-callable function)
  - range_contained_by_internal (used for symmetric containment test)
  - range_gist_consistent_int_range (GiST index support)
  - range_gist_consistent_leaf_range (GiST index support)
  - spg_range_quad_leaf_consistent (SP-GiST index support)

## Notes and Other Information
- This is an internal function used by various range operators and index support functions
- Returns true if r1 contains r2, false otherwise
- Handles edge cases: empty r2 is contained by any r1, empty r1 contains no non-empty r2
- Uses type-safe comparison through the typcache mechanism
- Critical for implementing the @> (contains) and <@ (contained by) operators
- Used extensively in range indexing strategies for query optimization
- Assumes caller has verified that both ranges are of compatible types