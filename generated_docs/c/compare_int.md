# compare_int

## Location
[src/backend/utils/adt/tsvector_op.c:433-441](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L433-L441)

## Overview
A static qsort comparator function that compares two integer values for sorting purposes.

## Definition

```c
static int
compare_int(const void *va, const void *vb)
```
## Detailed Description
The  function serves as a comparator function for the standard C library  function. It takes two void pointers (as required by qsort's interface), dereferences them as integers, and returns a comparison result. This function enables sorting of integer arrays in ascending order.

The function follows the standard comparator contract:
- Returns a negative value if the first integer is less than the second
- Returns zero if both integers are equal  
- Returns a positive value if the first integer is greater than the second

It uses PostgreSQL's  function to perform the actual comparison, which provides consistent and safe 32-bit signed integer comparison.

## Parameters / Member Variables
- : Pointer to the first integer to compare (cast from void*)
- : Pointer to the second integer to compare (cast from void*)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cmp_s32](../p/pg_cmp_s32.md) (PostgreSQL's 32-bit signed integer comparison function)
- Called from:
  - [tsvector_delete_by_indices](../t/tsvector_delete_by_indices.md) (used with qsort to sort index arrays)

## Notes and Other Information
- Designed specifically for use with qsort and similar sorting functions that require comparator callbacks
- Uses PostgreSQL's standard integer comparison function for consistency
- Part of the TSVector deletion infrastructure where integer indices need to be sorted
- The void pointer interface is mandated by the qsort function signature
- Provides ascending order sorting when used with qsort