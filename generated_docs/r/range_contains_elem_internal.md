# range_contains_elem_internal

## Location
[src/backend/utils/adt/rangetypes.c:2627-2674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L2627-L2674)

## Overview
Tests whether a range contains a specific element value by comparing the element against the range's lower and upper bounds, respecting bound inclusivity and infinity.

## Definition
```c
bool range_contains_elem_internal(TypeCacheEntry *typcache, const RangeType *r, Datum val)
```

## Detailed Description
The `range_contains_elem_internal` function determines if a given element value falls within the bounds of a range. It deserializes the range to extract its bounds and tests the element against both the lower and upper bounds using type-specific comparison functions. The function handles infinite bounds (unbounded ranges) and respects the inclusivity/exclusivity of bounds when the element value exactly matches a bound.

The containment logic follows standard mathematical interval notation: for inclusive bounds, equality means containment, while for exclusive bounds, equality means non-containment. Empty ranges never contain any element.

## Parameters / Member Variables
- `typcache`: Type cache entry containing comparison functions and collation information for the range element type
- `r`: The range to test for element containment
- `val`: The element value (as a Datum) to test for containment within the range

## Dependencies
- Functions called/Symbols referenced:
  - RangeBound (struct type for representing range bounds)
  - [range_deserialize](range_deserialize.md) (function to extract bounds and empty flag from range)
  - [DatumGetInt32](../D/DatumGetInt32.md) (function to extract int32 from Datum)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (PostgreSQL function call mechanism with collation support)
- Called from (representative examples):
  - [range_contains_elem](range_contains_elem.md) (public SQL-callable function)
  - [elem_contained_by_range](../e/elem_contained_by_range.md) (symmetric element containment function)
  - [range_gist_consistent_int_element](range_gist_consistent_int_element.md) (GiST index support for element queries)
  - [range_gist_consistent_leaf_element](range_gist_consistent_leaf_element.md) (GiST index support for leaf element queries)
  - [spg_range_quad_leaf_consistent](../s/spg_range_quad_leaf_consistent.md) (SP-GiST index support)

## Notes and Other Information
- This is an internal function used to implement element-in-range operations
- Returns true if the range contains the element, false otherwise
- Empty ranges never contain any element (returns false immediately)
- Handles infinite bounds by skipping comparison when bounds are infinite
- Uses type-specific comparison through the typcache's rng_cmp_proc_finfo function
- Supports collation-aware comparisons for text and other collatable types
- Critical for implementing the @> (contains element) and <@ (element contained by) operators
- Used extensively in range indexing for efficient element-in-range queries
- Bound inclusivity is strictly enforced: exclusive bounds exclude exact matches

## Simplified Source

```c
bool range_contains_elem_internal(TypeCacheEntry *typcache, const RangeType *r, Datum val) {
    RangeBound lower, upper;
    bool empty;
    int32 cmp;

    // Extract range bounds
    range_deserialize(typcache, r, &lower, &upper, &empty);

    // Empty ranges contain no elements
    if (empty)
        return false;

    // Check lower bound constraint
    if (!lower.infinite) {
        cmp = DatumGetInt32(FunctionCall2Coll(&typcache->rng_cmp_proc_finfo,
                                             typcache->rng_collation,
                                             lower.val, val));
        if (cmp > 0)
            return false;  // val < lower bound
        if (cmp == 0 && !lower.inclusive)
            return false;  // val == lower bound but bound is exclusive
    }

    // Check upper bound constraint
    if (!upper.infinite) {
        cmp = DatumGetInt32(FunctionCall2Coll(&typcache->rng_cmp_proc_finfo,
                                             typcache->rng_collation,
                                             upper.val, val));
        if (cmp < 0)
            return false;  // val > upper bound
        if (cmp == 0 && !upper.inclusive)
            return false;  // val == upper bound but bound is exclusive
    }

    return true;
}
```