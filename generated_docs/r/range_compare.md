# range_compare

## Location
[src/backend/utils/adt/rangetypes.c:2129-2164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L2129-L2164)

## Overview
A qsort-compatible comparison function for sorting arrays of RangeType pointers, implementing a total ordering where empty ranges sort before non-empty ranges.

## Definition

```c
int
range_compare(const void *key1, const void *key2, void *arg)
```
## Detailed Description
The `range_compare` function serves as a callback for qsort operations on arrays of RangeType pointers. It implements a well-defined total ordering for ranges: empty ranges are considered equal to each other and sort before any non-empty ranges. For non-empty ranges, the primary sort key is the lower bound, with the upper bound serving as a tiebreaker when lower bounds are equal. This ordering is essential for range canonicalization operations and ensures consistent, predictable sorting behavior for range arrays.

## Parameters / Member Variables
- `key1`: Pointer to the first RangeType pointer being compared
- `key2`: Pointer to the second RangeType pointer being compared  
- `arg`: TypeCacheEntry pointer providing type-specific comparison functions

## Dependencies
- Functions called/Symbols referenced:
  - [range_deserialize](range_deserialize.md)
  - [range_cmp_bounds](range_cmp_bounds.md)
- Called from (representative examples):
  - [multirange_canonicalize](../m/multirange_canonicalize.md)

## Notes and Other Information
- Designed specifically as a qsort callback function with the standard three-parameter signature
- Empty ranges always compare as equal (return 0) regardless of their internal representation
- Empty ranges sort to the left (return -1) of any non-empty range for consistent ordering
- For non-empty ranges, uses lexicographic ordering: lower bound first, then upper bound
- Critical for multirange canonicalization where ranges must be sorted and merged
- The function expects key1 and key2 to be pointers to RangeType pointers (double indirection)

## Simplified Source

```c
int
range_compare(const void *key1, const void *key2, void *arg)
{
    RangeType *r1 = *(RangeType **) key1;
    RangeType *r2 = *(RangeType **) key2;
    TypeCacheEntry *typcache = (TypeCacheEntry *) arg;

    // Deserialize both ranges to get bounds and empty status
    RangeBound lower1, upper1, lower2, upper2;
    bool empty1, empty2;
    range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
    range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

    // Handle empty ranges: empty == empty, empty < non-empty
    if (empty1 && empty2)
        return 0;
    else if (empty1)
        return -1;
    else if (empty2)
        return 1;

    // Both non-empty: compare lower bounds first, then upper bounds
    int cmp = range_cmp_bounds(typcache, &lower1, &lower2);
    if (cmp == 0)
        cmp = range_cmp_bounds(typcache, &upper1, &upper2);

    return cmp;
}
```