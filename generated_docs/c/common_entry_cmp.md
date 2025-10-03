# common_entry_cmp

## Location
[src/backend/utils/adt/rangetypes_gist.c:1770-1787](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L1770-L1787)

## Overview
A comparison function that compares two CommonEntry structures based on their delta values for use in GiST index splitting algorithms.

## Definition

```c
static int
common_entry_cmp(const void *i1, const void *i2)
```
## Detailed Description
This function serves as a comparator for sorting CommonEntry structures during GiST (Generalized Search Tree) index splitting operations. It extracts the delta values from two CommonEntry structures and compares them using PostgreSQL's internal float8 comparison logic.

The function follows the standard qsort comparator interface, returning a negative value if the first entry's delta is smaller, zero if they are equal, and a positive value if the first is larger. The delta values likely represent some form of metric used to determine optimal splitting strategies during index node division.

## Parameters / Member Variables
- `*i1`: Pointer to the first CommonEntry structure to compare
- `*i2`: Pointer to the second CommonEntry structure to compare
## Dependencies
- Functions called/Symbols referenced:
  - CommonEntry (struct type)
  - [float8_cmp_internal](../f/float8_cmp_internal.md)
- Called from:
  - PLACE_RIGHT (src/backend/access/gist/gistproc.c:808)
  - rangeCopy (src/backend/utils/adt/rangetypes_gist.c:184)
  - [range_gist_double_sorting_split](../r/range_gist_double_sorting_split.md) (src/backend/utils/adt/rangetypes_gist.c:1589)

## Notes and Other Information
- This is a static function used internally within GiST indexing operations
- Designed specifically for use with qsort or similar sorting algorithms
- Part of the GiST splitting strategy to optimize index node division based on delta metrics
- The comparison uses float8 data type for the delta values
- Used in both general GiST operations (PLACE_RIGHT context) and range type indexing contexts
- The delta values likely represent cost metrics or distance measures used in splitting decisions