# cmp_list_len_asc

## Location
[src/backend/parser/parse_agg.c:1759-1768](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L1759-L1768)

## Overview
A comparator function for sorting lists by their length in ascending order, used with PostgreSQL's list_sort function.

## Definition

```c
static int
cmp_list_len_asc(const ListCell *a, const ListCell *b)
```
## Detailed Description
This function implements a comparison callback for sorting operations on lists of lists. It compares two ListCell pointers that contain List structures and returns an integer indicating their relative ordering based on list length:

- Returns negative value if list a is shorter than list b
- Returns zero if both lists have the same length  
- Returns positive value if list a is longer than list b

The function is designed to be used with PostgreSQL's list_sort function to arrange grouping sets in order from shortest to longest, which is important for optimizing GROUP BY processing and ensuring consistent output ordering.

## Parameters / Member Variables
- `*a`: ListCell pointer containing the first List to compare
- `*b`: ListCell pointer containing the second List to compare
## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md): Gets the number of elements in a list
  - lfirst: Extracts the datum from a ListCell
  - [pg_cmp_s32](../p/pg_cmp_s32.md): PostgreSQL's 32-bit integer comparison function
- Called from:
  - [expand_grouping_sets](../e/expand_grouping_sets.md): Uses this to sort grouping combinations by size
  - [cmp_list_len_contents_asc](cmp_list_len_contents_asc.md): Uses this as part of a two-level comparison

## Notes and Other Information
- This is a standard comparator function following the C library qsort convention
- The ascending order ensures shorter grouping sets are processed first, which can improve query optimization
- Part of PostgreSQL's GROUPING SETS implementation for organizing expanded grouping combinations
- Uses PostgreSQL's standard comparison utilities for consistent behavior across the codebase

## Simplified Source

```c
static int
cmp_list_len_asc(const ListCell *a, const ListCell *b)
{
    int la = list_length((const List *) lfirst(a));
    int lb = list_length((const List *) lfirst(b));

    return pg_cmp_s32(la, lb);
}
```