# pg_cmp_u16

## Location
[src/include/common/int.h:477-482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int.h#L477-L482)

## Overview
An unsigned 16-bit integer comparison function designed for use in qsort() comparator functions, returning values compatible with standard comparison semantics.

## Definition
```c
static inline int pg_cmp_u16(uint16 a, uint16 b)
```

## Detailed Description
This inline function compares two unsigned 16-bit integers and returns an integer indicating their relative ordering. It follows the standard qsort() comparator convention: returning a positive value if `a > b`, zero if `a == b`, and a negative value if `a < b`.

The implementation uses the same efficient technique as its signed counterpart (`pg_cmp_s16`), casting both 16-bit values to signed 32-bit integers before performing subtraction. This approach prevents overflow while maintaining correct comparison semantics. Since both values are unsigned and fit within the positive range of a 32-bit signed integer, the subtraction result will always be within the valid range and provide the correct ordering relationship.

This function is part of PostgreSQL's family of safe comparison routines designed to ensure transitivity in sorting operations and eliminate overflow-related bugs.

## Parameters / Member Variables
- `a`: First unsigned 16-bit integer to compare
- `b`: Second unsigned 16-bit integer to compare

## Dependencies
- Functions called/Symbols referenced:
  - None (performs direct arithmetic operations)
- Called from (representative examples):
  - [cmpOffsetNumbers](../c/cmpOffsetNumbers.md) at src/backend/access/heap/vacuumlazy.c:1391
  - [cmpOffsetNumbers](../c/cmpOffsetNumbers.md) at src/backend/access/spgist/spgdoinsert.c:114

## Notes and Other Information
- The function is designed for use in qsort() comparator functions and other sorting contexts requiring offset number comparisons
- The cast to `int32` prevents overflow issues that could occur with naive unsigned integer subtraction comparisons
- Both referenced usage locations involve `cmpOffsetNumbers` functions, indicating this is commonly used for comparing page offset numbers in PostgreSQL's storage system
- This approach ensures comparator transitivity, which is essential for stable and correct sorting algorithms
- The implementation handles the full range of 16-bit unsigned values (0 to 65535) correctly
- Part of PostgreSQL's comprehensive suite of safe comparison utilities used throughout the database engine