# make_empty_range

## Location
[src/backend/utils/adt/rangetypes.c:2165-2186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L2165-L2186)

## Overview
Creates and returns an empty range of the specified type by constructing dummy bounds and calling make_range with the empty flag set to true.

## Definition

```c
RangeType *
make_empty_range(TypeCacheEntry *typcache)
```
## Detailed Description
The `make_empty_range` function provides a convenient way to create an empty range of any range type. It constructs dummy lower and upper bounds with default values (both set to 0, non-infinite, non-inclusive) and then calls `make_range` with the empty flag set to true. Since the range is explicitly marked as empty, the actual bound values are irrelevant and ignored. This function is essential for range operations that need to return empty results, such as when ranges don't intersect or when performing set operations that yield no result.

## Parameters / Member Variables
- `typcache`: Type cache entry specifying the range type for which to create an empty range

## Dependencies
- Functions called/Symbols referenced:
  - [make_range](make_range.md)
- Called from (representative examples):
  - [range_minus_internal](../r/range_minus_internal.md)
  - [range_intersect_internal](../r/range_intersect_internal.md)
  - [multirange_get_union_range](multirange_get_union_range.md)
  - [multirange_agg_transfn](multirange_agg_transfn.md)
  - [range_merge_from_multirange](../r/range_merge_from_multirange.md)

## Notes and Other Information
- The dummy bound values (both set to 0) are meaningless since the range is marked as empty
- Empty ranges have a standardized representation regardless of the input bound values
- This function is a convenience wrapper around `make_range` for the common case of creating empty ranges
- Empty ranges are crucial for representing "no result" conditions in range arithmetic operations
- The function always passes NULL as the error context, indicating hard error handling rather than soft errors

## Simplified Source

```c
RangeType *
make_empty_range(TypeCacheEntry *typcache)
{
    // Create dummy bounds (values don't matter since range will be empty)
    RangeBound lower = {
        .val = (Datum) 0,
        .infinite = false,
        .inclusive = false,
        .lower = true
    };

    RangeBound upper = {
        .val = (Datum) 0,
        .infinite = false,
        .inclusive = false,
        .lower = false
    };

    // Create empty range using make_range with empty=true
    return make_range(typcache, &lower, &upper, true, NULL);
}
```