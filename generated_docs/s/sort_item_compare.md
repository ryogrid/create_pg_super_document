# sort_item_compare

## Location
[src/backend/statistics/mcv.c:465-489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L465-L489)

## Overview
A comparison function for sorting SortItem objects based on a single column value, used with qsort for single-dimensional sorting operations.

## Definition
```c
static int sort_item_compare(const void *a, const void *b, void *arg)
```

## Detailed Description
This function provides a comparison mechanism for SortItem objects when sorting by a single column. It uses the PostgreSQL sort comparator framework to compare the first column values of two SortItem objects, properly handling NULL values according to the sort specification. The function follows the standard qsort comparison interface, returning negative, zero, or positive values to indicate ordering relationships.

## Parameters / Member Variables
- `a`: First SortItem object to compare (cast from void pointer)
- `b`: Second SortItem object to compare (cast from void pointer) 
- `arg`: SortSupport structure containing sort specifications and comparison function

## Dependencies
- Functions called/Symbols referenced:
  - [ApplySortComparator](../A/ApplySortComparator.md)
  - [SortSupport](../S/SortSupport.md) (type)
  - [SortItem](../S/SortItem.md) (type)
- Called from (representative examples):
  - [build_column_frequencies](../b/build_column_frequencies.md)

## Notes and Other Information
- Designed for single-column sorting operations (uses values[0] and isnull[0])
- Properly handles NULL values according to PostgreSQL sorting semantics
- Used as a callback function with qsort_interruptible for sorting operations
- Part of the MCV statistics building infrastructure for frequency analysis

## Simplified Source
```c
static int sort_item_compare(const void *a, const void *b, void *arg) {
    SortSupport ssup = (SortSupport) arg;
    SortItem *item_a = (SortItem *) a;
    SortItem *item_b = (SortItem *) b;

    // Compare first column values using PostgreSQL sort comparator
    return ApplySortComparator(item_a->values[0], item_a->isnull[0],
                              item_b->values[0], item_b->isnull[0],
                              ssup);
}
```