# multi_sort_compare

## Location
[src/backend/statistics/extended_stats.c:865-889](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L865-L889)

## Overview
A comparison function for multi-dimensional sorting that compares two SortItem structures across all dimensions in sequential order, returning the first non-zero comparison result.

## Definition

```c
int
multi_sort_compare(const void *a, const void *b, void *arg)
```
## Detailed Description
This function implements a lexicographic comparison for multi-dimensional sorting operations. It iterates through all dimensions of two SortItem structures, applying the appropriate comparison function for each dimension. The function returns as soon as it finds a non-zero comparison result, implementing a proper ordering for multi-column sort operations. This is essential for PostgreSQL's extended statistics system where data needs to be sorted across multiple columns simultaneously for statistical analysis.

## Parameters / Member Variables
- `*a`: Pointer to the first SortItem structure to compare (cast from const void*)
- `*b`: Pointer to the second SortItem structure to compare (cast from const void*)
- `*arg`: Pointer to MultiSortSupport structure containing sort configuration for all dimensions (cast from void*)
## Dependencies
- Functions called/Symbols referenced:
  - [ApplySortComparator](../A/ApplySortComparator.md)
  - MultiSortSupport (type)
  - [SortItem](../S/SortItem.md) (type)
- Called from (representative examples):
  - [build_sorted_items](../b/build_sorted_items.md) (src/backend/statistics/extended_stats.c:1108)
  - [statext_mcv_build](../s/statext_mcv_build.md) (src/backend/statistics/mcv.c:326)
  - [count_distinct_groups](../c/count_distinct_groups.md) (src/backend/statistics/mcv.c:388, 390)
  - [build_distinct_groups](../b/build_distinct_groups.md) (src/backend/statistics/mcv.c:440, 443)
  - [ndistinct_for_combination](../n/ndistinct_for_combination.md) (src/backend/statistics/mvdistinct.c:492, 501)

## Notes and Other Information
- Follows the standard qsort() comparison function interface (const void*, const void*, void*)
- Returns 0 when all dimensions are equal, negative when first item is less, positive when first item is greater
- Uses ApplySortComparator which handles NULL value comparisons according to the configured null ordering
- Critical component for multi-variate statistical analysis in PostgreSQL's ANALYZE process
- The function assumes SortItem structures have been properly initialized with values and null indicators

## Simplified Source

```c
int
multi_sort_compare(const void *a, const void *b, void *arg)
{
    MultiSortSupport mss = (MultiSortSupport) arg;
    SortItem *ia = (SortItem *) a;
    SortItem *ib = (SortItem *) b;
    int i;

    // Compare each dimension in order
    for (i = 0; i < mss->ndims; i++)
    {
        int compare;

        // Apply comparison function for this dimension
        compare = ApplySortComparator(ia->values[i], ia->isnull[i],
                                    ib->values[i], ib->isnull[i],
                                    &mss->ssup[i]);

        // Return first non-zero comparison result
        if (compare != 0)
            return compare;
    }

    // All dimensions are equal
    return 0;
}
```