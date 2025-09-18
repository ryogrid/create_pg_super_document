# spg_range_quad_leaf_consistent

## Location
src/backend/utils/adt/rangetypes_spgist.c: 917 - 998

## Overview
SP-GiST leaf node consistent function that performs final range comparisons between indexed range values and query conditions.

## Definition


## Detailed Description
This function implements the leaf node consistent logic for SP-GiST indexing of PostgreSQL range types. It represents the final step in index traversal where actual range values stored in leaf nodes are compared against query conditions to determine if they satisfy the search criteria.

The function iterates through all scan keys (query conditions) and applies the appropriate range comparison function based on the strategy:

- **RANGESTRAT_BEFORE**: Checks if leaf range is entirely before query range
- **RANGESTRAT_OVERLEFT**: Checks if leaf range overlaps or is left of query range  
- **RANGESTRAT_OVERLAPS**: Checks if leaf range overlaps with query range
- **RANGESTRAT_OVERRIGHT**: Checks if leaf range overlaps or is right of query range
- **RANGESTRAT_AFTER**: Checks if leaf range is entirely after query range
- **RANGESTRAT_ADJACENT**: Checks if leaf range is adjacent to query range
- **RANGESTRAT_CONTAINS**: Checks if leaf range contains query range
- **RANGESTRAT_CONTAINED_BY**: Checks if leaf range is contained by query range
- **RANGESTRAT_CONTAINS_ELEM**: Checks if leaf range contains query element
- **RANGESTRAT_EQ**: Checks if leaf range equals query range

All comparisons are exact (no recheck required), and the function returns true only if all query conditions are satisfied.

## Parameters / Member Variables
- : Input structure containing the leaf datum and scan keys
- : Output structure for returning results and recheck flag

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetRangeTypeP
  - range_get_typcache
  - RangeTypeGetOid
  - range_before_internal
  - range_overleft_internal
  - range_overlaps_internal
  - range_overright_internal
  - range_after_internal
  - range_adjacent_internal
  - range_contains_internal
  - range_contained_by_internal
  - range_contains_elem_internal
  - range_eq_internal
- Called from (representative examples):
  - No direct references found (likely called through function pointer in SP-GiST operator class)

## Notes and Other Information
- Sets  since all tests are exact and don't require revalidation
- Returns the original leaf datum as  for result retrieval
- Short-circuits on first non-matching condition to avoid unnecessary comparisons
- Part of the complete SP-GiST range indexing implementation alongside inner node functions
- Critical for the final filtering step in range-based index searches