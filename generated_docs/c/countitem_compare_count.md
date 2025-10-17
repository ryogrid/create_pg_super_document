# countitem_compare_count

## Location
[src/backend/utils/adt/array_typanalyze.c:780-791](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_typanalyze.c#L780-L791)

## Overview
A static comparison function used to sort DECountItem pointers in ascending order of their count values for array distinct element statistics collection.

## Definition

```c
static int
countitem_compare_count(const void *e1, const void *e2, void *arg)
```
## Detailed Description
This function implements a comparator for sorting DECountItem (Distinct Element Count Item) structures based on their count field in ascending order (lowest count first). It follows the standard C library qsort comparison function interface and is specifically designed for use in PostgreSQL's array statistics collection system. The function performs explicit three-way comparison logic rather than simple subtraction to avoid potential integer overflow issues that could occur with very large count values.

This comparator is used to organize distinct element count statistics, which helps the query planner estimate the selectivity of array operations and optimize query execution plans.

## Parameters / Member Variables
- `*e1`: Pointer to the first DECountItem pointer to compare (cast from const void*)
- `*e2`: Pointer to the second DECountItem pointer to compare (cast from const void*)
- `*arg`: Unused context argument (required by qsort interface but not used in this function)
## Dependencies
- Functions called/Symbols referenced:
  - DECountItem (structure being compared)
- Called from (representative examples):
  - [compute_array_stats](compute_array_stats.md) (for sorting distinct element count statistics)

## Notes and Other Information
- Returns -1 if e1's count < e2's count (ascending sort)
- Returns 1 if e1's count > e2's count
- Returns 0 if counts are equal
- Uses explicit comparison logic instead of subtraction to prevent integer overflow
- Part of PostgreSQL's array statistics collection system for query optimization
- Used to organize distinct element counts for histogram and selectivity estimation purposes
- The function operates on pointers to DECountItem pointers, requiring double dereferencing
- Critical for creating accurate statistics about array element distribution patterns

## Simplified Source

```c
static int countitem_compare_count(const void *e1, const void *e2, void *arg) {
    // Cast to DECountItem pointers and extract count values
    const DECountItem *const *t1 = (const DECountItem *const *) e1;
    const DECountItem *const *t2 = (const DECountItem *const *) e2;

    // Compare counts in ascending order (lowest first)
    if ((*t1)->count < (*t2)->count)
        return -1;
    else if ((*t1)->count == (*t2)->count)
        return 0;
    else
        return 1;
}
```