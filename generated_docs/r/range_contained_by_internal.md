# range_contained_by_internal

## Location
[src/backend/utils/adt/rangetypes.c:2618-2626](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L2618-L2626)

## Overview
Tests whether one range is contained by another range by delegating to range_contains_internal with swapped arguments, implementing the symmetric contained-by operation.

## Definition
```c
bool range_contained_by_internal(TypeCacheEntry *typcache, const RangeType *r1, const RangeType *r2)
```

## Detailed Description
The `range_contained_by_internal` function provides a simple wrapper around `range_contains_internal` to implement the "contained by" relationship. It swaps the order of the range arguments to test whether r1 is contained by r2, which is equivalent to testing whether r2 contains r1. This function exists to provide a more intuitive interface for the contained-by operation while reusing the core containment logic.

This design pattern avoids code duplication by leveraging the symmetry of the containment relationship: "A is contained by B" is logically equivalent to "B contains A".

## Parameters / Member Variables
- `typcache`: Type cache entry containing comparison functions and type information for the range element type
- `r1`: The potentially contained range (left operand)
- `r2`: The potentially containing range (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - [range_contains_internal](range_contains_internal.md) (core containment testing function with swapped arguments)
- Called from (representative examples):
  - [range_contained_by](range_contained_by.md) (public SQL-callable function)
  - [range_gist_consistent_leaf_range](range_gist_consistent_leaf_range.md) (GiST index support)
  - [spg_range_quad_leaf_consistent](../s/spg_range_quad_leaf_consistent.md) (SP-GiST index support)
  - RANGESTRAT_EQ (macro for range strategy operations)

## Notes and Other Information
- This is a thin wrapper function that provides semantic clarity for contained-by operations
- Returns true if r1 is contained by r2, false otherwise
- Inherits all the behavior and edge case handling from range_contains_internal
- Used to implement the <@ (contained by) operator in PostgreSQL
- Essential for range indexing strategies and query optimization
- Demonstrates good software engineering practice by reusing existing logic rather than duplicating it
- The parameter order matches the expected semantics: r1 <@ r2 (r1 is contained by r2)

## Simplified Source

```c
bool range_contained_by_internal(TypeCacheEntry *typcache, const RangeType *r1, const RangeType *r2) {
    // Simply swap arguments and call range_contains_internal
    // "r1 contained by r2" is equivalent to "r2 contains r1"
    return range_contains_internal(typcache, r2, r1);
}
```