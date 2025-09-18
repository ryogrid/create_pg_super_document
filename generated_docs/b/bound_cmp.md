# bound_cmp

## Location
[src/backend/utils/adt/rangetypes_spgist.c:186-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_spgist.c#L186-L199)

## Overview
Comparison function for sorting range bounds, used as a callback for qsort operations in SP-GiST range quadtree splitting.

## Definition
static int bound_cmp(const void *a, const void *b, void *arg)

## Detailed Description
This function provides a comparison interface for sorting RangeBound structures during the picksplit operation of SP-GiST range indexing. It serves as a wrapper around the range_cmp_bounds function, adapting it for use with qsort_arg. The function is essential for finding median values of range bounds when constructing centroid ranges during node splitting operations.

## Parameters / Member Variables
- a: Pointer to the first RangeBound to compare
- b: Pointer to the second RangeBound to compare  
- arg: TypeCacheEntry pointer passed as context for range comparisons

## Dependencies
- Functions called/Symbols referenced:
  - RangeBound (structure type)
  - [TypeCacheEntry](../T/TypeCacheEntry.md) (structure type)
  - [range_cmp_bounds](../r/range_cmp_bounds.md) (range bound comparison function)
- Called from (representative examples):
  - [spg_range_quad_picksplit](../s/spg_range_quad_picksplit.md) (via qsort_arg for sorting bounds)

## Notes and Other Information
- Designed specifically for use with qsort_arg function which requires this exact signature
- Acts as an adapter between qsort requirements and PostgreSQL's range comparison functions
- Critical for median calculation in quadtree centroid selection
- Uses TypeCacheEntry to provide type-specific comparison logic for different range types
- Returns standard comparison result: negative, zero, or positive integer
- Located in src/backend/utils/adt/rangetypes_spgist.c:186-199