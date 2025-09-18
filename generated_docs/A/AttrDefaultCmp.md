# AttrDefaultCmp

## Location
src/backend/utils/cache/relcache.c: 4570 - 4584

## Overview
A qsort comparator function that sorts AttrDefault entries by attribute number (adnum) in ascending order.

## Definition
```c
static int AttrDefaultCmp(const void *a, const void *b)
```

## Detailed Description
AttrDefaultCmp is a utility function designed specifically for use with qsort() to order AttrDefault structures by their attribute numbers. The function follows the standard qsort comparator contract, returning a negative value if the first element should come before the second, zero if they are equal, and a positive value if the first element should come after the second.

The comparison is performed using PostgreSQL's pg_cmp_s16 function, which provides a safe comparison of 16-bit signed integers. This ensures that the AttrDefault array is sorted in ascending order by attribute number, which facilitates efficient searching and provides a consistent ordering for functions like equalTupleDescs() that need to compare attribute default arrays.

## Parameters
- `a`: Pointer to the first AttrDefault structure to compare
- `b`: Pointer to the second AttrDefault structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - [AttrDefault](AttrDefault.md) (structure type)
  - [pg_cmp_s16](../p/pg_cmp_s16.md)
- Called from:
  - [AttrDefaultFetch](AttrDefaultFetch.md)

## Notes and Other Information
- Used exclusively by AttrDefaultFetch for sorting the loaded default value array
- Follows standard qsort comparator conventions for return values
- Sorts in ascending order by attribute number (adnum field)
- Enables efficient binary search and comparison operations on AttrDefault arrays
- Part of the relcache infrastructure for organizing attribute default information