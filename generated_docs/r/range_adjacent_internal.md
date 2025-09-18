# range_adjacent_internal

## Location
src/backend/utils/adt/rangetypes.c: 798 - 827

## Overview
Determines if two ranges are adjacent (touching but not overlapping) by checking if their boundaries meet without gaps or overlaps.

## Definition


## Detailed Description
This function implements the core logic for range adjacency testing. Two ranges are considered adjacent if they touch at exactly one boundary point but do not overlap. The function works by deserializing both ranges into their boundary components and then checking if either the upper bound of the first range is adjacent to the lower bound of the second range, or vice versa.

The adjacency test is bidirectional: ranges A..B and C..D are adjacent if B is adjacent to C OR if D is adjacent to A. This handles cases where ranges might be passed in either order.

## Parameters / Member Variables
- : TypeCacheEntry containing range type metadata and comparison functions
- : First RangeType to test for adjacency
- : Second RangeType to test for adjacency

## Dependencies
- Functions called/Symbols referenced:
  - RangeTypeGetOid
  - range_deserialize
  - bounds_adjacent
- Called from (representative examples):
  - range_adjacent
  - range_union_internal
  - multirange_canonicalize
  - range_gist_consistent_int_range
  - range_gist_consistent_leaf_range
  - spg_range_quad_leaf_consistent

## Notes and Other Information
- Validates that both ranges are of the same type, throwing an error if not
- Empty ranges are never considered adjacent to any other range
- The function is internal and used by both the SQL-callable range_adjacent function and internal range operations
- Critical for range union operations and GiST/SP-GiST index operations
- Uses the bounds_adjacent function to perform the actual boundary adjacency tests
- Located in src/backend/utils/adt/rangetypes.c:798-827