# range_adjacent_internal

## Location
[src/backend/utils/adt/rangetypes.c:798-827](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L798-L827)

## Overview
Determines if two ranges are adjacent (touching but not overlapping) by checking if their boundaries meet without gaps or overlaps.

## Definition

```c
bool
range_adjacent_internal(TypeCacheEntry *typcache, const RangeType *r1, const RangeType *r2)
```
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
  - [range_deserialize](range_deserialize.md)
  - [bounds_adjacent](../b/bounds_adjacent.md)
- Called from (representative examples):
  - [range_adjacent](range_adjacent.md)
  - [range_union_internal](range_union_internal.md)
  - [multirange_canonicalize](../m/multirange_canonicalize.md)
  - [range_gist_consistent_int_range](range_gist_consistent_int_range.md)
  - [range_gist_consistent_leaf_range](range_gist_consistent_leaf_range.md)
  - [spg_range_quad_leaf_consistent](../s/spg_range_quad_leaf_consistent.md)

## Notes and Other Information
- Validates that both ranges are of the same type, throwing an error if not
- Empty ranges are never considered adjacent to any other range
- The function is internal and used by both the SQL-callable range_adjacent function and internal range operations
- Critical for range union operations and GiST/SP-GiST index operations
- Uses the bounds_adjacent function to perform the actual boundary adjacency tests
- Located in src/backend/utils/adt/rangetypes.c:798-827