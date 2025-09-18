# int128_compare

## Location
[src/include/common/int128.h:71-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int128.h#L71-L83)

## Overview
Compares two 128-bit integer values and returns a standard comparison result (-1, 0, or +1).

## Definition
```c
static inline int
int128_compare(INT128 x, INT128 y)
```

## Detailed Description
This function performs a three-way comparison between two 128-bit integers, following the standard comparison convention used throughout PostgreSQL. It returns -1 if the first value is less than the second, +1 if the first value is greater than the second, and 0 if the values are equal. The implementation uses simple conditional checks with the underlying 128-bit integer comparison operators.

## Parameters / Member Variables
- `x`: First INT128 value to compare.
- `y`: Second INT128 value to compare.

## Dependencies
- Functions called/Symbols referenced:
  - INT128 (type definition)
- Called from (representative examples):
  - [interval_cmp_internal](interval_cmp_internal.md) (in src/backend/utils/adt/timestamp.c:2510)
  - [interval_sign](interval_sign.md) (in src/backend/utils/adt/timestamp.c:2519)
  - [main](../m/main.md) (in src/tools/testint128.c:144, 148, 158, 162)

## Notes and Other Information
- This is a static inline function defined in the header file for optimal performance
- Returns values follow the standard C library comparison convention (strcmp-style)
- Used extensively in PostgreSQL's timestamp and interval arithmetic operations
- The function parameters are passed by value rather than by reference, unlike the addition functions
- Essential for sorting, ordering, and conditional operations involving 128-bit integers
- Part of PostgreSQL's 128-bit integer arithmetic library for handling large numeric calculations