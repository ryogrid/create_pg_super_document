# range_overleft_internal

## Location
src/backend/utils/adt/rangetypes.c: 887 - 914

## Overview
The range_overleft_internal function tests whether the first range does not extend to the right of the second range (i.e., the first range's upper bound is less than or equal to the second range's upper bound).

## Definition
```c
bool range_overleft_internal(TypeCacheEntry *typcache, const RangeType *r1, const RangeType *r2)
```

## Detailed Description
This internal function implements the "does not extend to right of" operation for range types. It compares the upper bounds of two ranges to determine if the first range (r1) does not extend beyond the upper bound of the second range (r2). The function performs type validation, deserializes both ranges into their constituent bounds, handles empty range cases, and compares the upper bounds using the type-specific comparison function.

The function returns true if r1's upper bound is less than or equal to r2's upper bound, effectively implementing the &< operator semantics.

## Parameters / Member Variables
- `typcache`: TypeCacheEntry pointer containing type-specific information for range operations
- `r1`: const RangeType pointer to the first range to compare
- `r2`: const RangeType pointer to the second range to compare

## Dependencies
- Functions called/Symbols referenced:
  - RangeTypeGetOid (to validate range types match)
  - elog (for error reporting when types don't match)
  - range_deserialize (to extract bounds from both ranges)
  - range_cmp_bounds (to compare the upper bounds)
- Called from (representative examples):
  - range_overleft (public wrapper function)
  - multirange_intersect_internal
  - range_gist_consistent_int_range
  - range_gist_consistent_leaf_range
  - spg_range_quad_leaf_consistent
  - RANGESTRAT_EQ (macro in rangetypes.h)

## Notes and Other Information
- Located in src/backend/utils/adt/rangetypes.c:887-914
- Returns false for empty ranges, as they are neither before nor after any other range
- Used internally for indexing operations (GiST and SP-GiST) and multirange operations
- The function ensures type safety by checking that both ranges have the same OID
- Part of the range operator family that includes overlaps, overleft, overright operations