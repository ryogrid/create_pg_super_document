# multirange_eq_internal

## Location
src/backend/utils/adt/multirangetypes.c: 1864 - 1900

## Overview
An internal function that tests whether two multiranges are equal by comparing their range counts and individual range bounds.

## Definition
```c
bool multirange_eq_internal(TypeCacheEntry *rangetyp,
                           const MultirangeType *mr1,
                           const MultirangeType *mr2)
```

## Detailed Description
This function implements equality comparison for multiranges by performing element-wise comparison of all ranges within the multiranges. It first validates that both multiranges are of the same type, then compares their range counts. If the counts match, it iterates through each range pair, extracting bounds and comparing them using range-specific comparison functions. The comparison is strict - both lower and upper bounds must match exactly for each corresponding range position. The function assumes multiranges are already in normalized form (sorted, non-overlapping, non-adjacent ranges).

## Parameters / Member Variables
- `rangetyp`: TypeCacheEntry pointer containing type-specific information for range operations
- `mr1`: const MultirangeType pointer to the first multirange to compare
- `mr2`: const MultirangeType pointer to the second multirange to compare

## Dependencies
- Functions called/Symbols referenced:
  - `MultirangeTypeGetOid` - Get the OID of multirange types for validation
  - `multirange_get_bounds` - Extract bounds from specific ranges within multiranges
  - `range_cmp_bounds` - Compare individual range bounds for equality
  - `RangeBound` - Structure for representing range boundaries
  - `elog` - PostgreSQL error logging function
- Called from (representative examples):
  - `multirange_eq` - Public SQL equality function wrapper
  - `multirange_ne_internal` - Negated equality for inequality operations

## Notes and Other Information
- Performs type checking to ensure both multiranges are of compatible types
- Uses O(n) comparison where n is the number of ranges in the multiranges
- Relies on the normalized property of multiranges for correctness
- Early termination optimization: returns false immediately on first mismatch
- Part of PostgreSQL's multirange comparison operator family
- The function assumes input multiranges are valid and properly constructed