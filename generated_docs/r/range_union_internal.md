# range_union_internal

## Location
[src/backend/utils/adt/rangetypes.c:1052-1097](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1052-L1097)

## Overview
The range_union_internal function computes the set union of two ranges, with optional strict mode that requires ranges to be adjacent or overlapping.

## Definition
RangeType *range_union_internal(TypeCacheEntry *typcache, RangeType *r1, RangeType *r2, bool strict)

## Detailed Description
This internal function implements the mathematical set union operation between two ranges (A ∪ B). It can operate in two modes: strict mode (where ranges must be adjacent or overlapping to produce a valid contiguous result) and non-strict mode (where any two ranges can be unioned, potentially creating gaps). The function determines the result bounds by taking the minimum lower bound and maximum upper bound from both input ranges. When strict mode is enabled, it validates that the resulting union would be contiguous.

## Parameters / Member Variables
- typcache: Type cache entry containing comparison functions for the range element type
- r1: First range operand
- r2: Second range operand  
- strict: Boolean flag - if true, requires ranges to be adjacent or overlapping

## Dependencies
- Functions called/Symbols referenced:
  - RangeBound
  - RangeTypeGetOid
  - [range_deserialize](range_deserialize.md)
  - [range_overlaps_internal](range_overlaps_internal.md)
  - [range_adjacent_internal](range_adjacent_internal.md)
  - [range_cmp_bounds](range_cmp_bounds.md)
  - [make_range](../m/make_range.md)
  - [DatumGetBool](../D/DatumGetBool.md)
- Called from (representative examples):
  - [range_union](range_union.md)
  - [range_merge](range_merge.md)
  - [multirange_canonicalize](../m/multirange_canonicalize.md)
  - RANGESTRAT_EQ

## Notes and Other Information
- Returns the other range unchanged if either input range is empty
- In strict mode, throws DATA_EXCEPTION if ranges are neither overlapping nor adjacent
- Validates that both ranges have the same type before processing
- [Result](../R/Result.md) bounds are computed by taking the minimum of lower bounds and maximum of upper bounds
- Essential component for range union operations and multirange canonicalization
- Used internally by other range operations that need union functionality