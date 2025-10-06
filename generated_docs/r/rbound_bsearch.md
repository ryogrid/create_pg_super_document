# rbound_bsearch

## Location
[src/backend/utils/adt/multirangetypes_selfuncs.c:739-767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes_selfuncs.c#L739-L767)

## Overview
Binary search function that finds the greatest index of a range bound in an array that is less than (or less than or equal to) a given range bound, used for scalar operator selectivity estimation in PostgreSQL range type statistics.

## Definition
```c
static int rbound_bsearch(TypeCacheEntry *typcache, const RangeBound *value, const RangeBound *hist, int hist_length, bool equal)
```

## Detailed Description
This function performs a binary search on an array of range bounds to locate histogram bins for interpolation during selectivity estimation. It implements a specialized binary search that returns the greatest index where the histogram bound is less than (or optionally less than or equal to) the target value. The function is crucial for range type selectivity calculations, helping the query planner estimate how many rows will match range-based predicates.

The search uses `range_cmp_bounds()` to compare range bounds, respecting the type-specific comparison semantics. When the `equal` flag is set, the comparison includes equality (≤), otherwise it uses strict inequality (<).

## Parameters / Member Variables
- `typcache`: Type cache entry containing comparison functions and metadata for the range type
- `value`: Target range bound to search for in the histogram
- `hist`: Array of range bounds representing histogram bin boundaries
- `hist_length`: Number of elements in the histogram array
- `equal`: Flag determining whether to include equality in the comparison (≤ vs <)

## Dependencies
- Functions called/Symbols referenced:
  - [range_cmp_bounds](range_cmp_bounds.md)
  - RangeBound
- Called from (representative examples):
  - [calc_hist_selectivity_scalar](../c/calc_hist_selectivity_scalar.md)
  - [calc_hist_selectivity_contained](../c/calc_hist_selectivity_contained.md)
  - [calc_hist_selectivity_contains](../c/calc_hist_selectivity_contains.md)

## Notes and Other Information
- Returns -1 if all histogram bounds are greater than (or greater than or equal to) the target value
- Used in both regular range types and multirange types selectivity estimation
- The binary search is optimized for finding histogram bin boundaries rather than exact matches
- Critical component of PostgreSQL's cost-based query optimization for range predicates

## Simplified Source

```c
static int
rbound_bsearch(TypeCacheEntry *typcache, const RangeBound *value, const RangeBound *hist,
               int hist_length, bool equal)
{
    int lower = -1, upper = hist_length - 1, cmp, middle;

    // Binary search for the greatest index where hist[index] < value (or <= if equal=true)
    while (lower < upper) {
        middle = (lower + upper + 1) / 2;
        cmp = range_cmp_bounds(typcache, &hist[middle], value);

        if (cmp < 0 || (equal && cmp == 0))
            lower = middle;  // hist[middle] is less than (or equal to) value
        else
            upper = middle - 1;  // hist[middle] is greater than value
    }

    return lower;  // Returns -1 if all histogram values are >= value
}
```