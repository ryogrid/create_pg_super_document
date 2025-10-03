# range_overlaps_internal

## Location
[src/backend/utils/adt/rangetypes.c:841-873](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L841-L873)

## Overview
Determines if two ranges overlap by checking if they share any common values within their boundaries.

## Definition

```c
bool
range_overlaps_internal(TypeCacheEntry *typcache, const RangeType *r1, const RangeType *r2)
```
## Detailed Description
This function implements the core logic for range overlap detection. Two ranges overlap if they share at least one common value. The function works by deserializing both ranges and checking if the lower bound of either range falls within the boundaries of the other range.

The overlap test uses two conditions:
1. If the lower bound of r1 is greater than or equal to the lower bound of r2 AND less than or equal to the upper bound of r2, then r1's start is within r2
2. If the lower bound of r2 is greater than or equal to the lower bound of r1 AND less than or equal to the upper bound of r1, then r2's start is within r1

If either condition is true, the ranges overlap.

## Parameters / Member Variables
- `*typcache`: TypeCacheEntry containing range type metadata and comparison functions
- `*r1`: First RangeType to test for overlap
- `*r2`: Second RangeType to test for overlap
## Dependencies
- Functions called/Symbols referenced:
  - RangeTypeGetOid
  - [range_deserialize](range_deserialize.md)  
  - [range_cmp_bounds](range_cmp_bounds.md)
- Called from (representative examples):
  - [range_overlaps](range_overlaps.md)
  - [range_union_internal](range_union_internal.md)
  - [range_intersect_internal](range_intersect_internal.md)
  - [multirange_minus_internal](../m/multirange_minus_internal.md)
  - [multirange_intersect_internal](../m/multirange_intersect_internal.md)
  - [range_gist_consistent_int_range](range_gist_consistent_int_range.md)
  - [range_gist_consistent_leaf_range](range_gist_consistent_leaf_range.md)
  - [spg_range_quad_leaf_consistent](../s/spg_range_quad_leaf_consistent.md)

## Notes and Other Information
- Validates that both ranges are of the same type, throwing an error if not
- Empty ranges never overlap with any other range
- The function is internal and widely used by range operations and indexing
- Critical for range intersection, union, and subtraction operations
- Used extensively in GiST and SP-GiST index consistency checking
- The overlap test is efficient, requiring only boundary comparisons without full range intersection
- Located in src/backend/utils/adt/rangetypes.c:841-873