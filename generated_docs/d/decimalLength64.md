# decimalLength64

## Location
[src/backend/utils/adt/numutils.c:63-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numutils.c#L63-L120)

## Overview
Calculates the number of decimal digits required to represent a 64-bit unsigned integer value.

## Definition
```c
static inline int decimalLength64(const uint64 v)
```

## Detailed Description
This function efficiently computes the decimal length (number of digits) of a 64-bit unsigned integer using the same bit manipulation technique as decimalLength32, but extended to handle the larger value range of 64-bit integers. It uses a mathematical approximation based on the position of the leftmost bit to estimate the base-10 logarithm, then validates the result using a lookup table of powers of ten.

The algorithm works by:
1. Finding the position of the leftmost set bit using `pg_leftmost_one_pos64()`
2. Converting this to an approximation of the base-10 logarithm using the formula `(leftmost_bit_pos + 1) * 1233 / 4096`
3. Adjusting the result by comparing the input value against the corresponding power of ten from the 64-bit powers table

This approach avoids expensive division operations and provides optimal performance for 64-bit numeric formatting operations.

## Parameters / Member Variables
- `v`: The 64-bit unsigned integer value for which to calculate the decimal length

## Dependencies
- Functions called/Symbols referenced:
  - [pg_leftmost_one_pos64](../p/pg_leftmost_one_pos64.md)
  - int8
- Called from (representative examples):
  - [pg_ulltoa_n](../p/pg_ulltoa_n.md)

## Notes and Other Information
- The function is marked `static inline` for optimal performance in numeric conversion routines
- Uses a precomputed array `PowersOfTen[]` containing powers of 10 from 10^0 to 10^19 using UINT64CONST macros
- The magic number 1233/4096 is the same rational approximation of log₂(10) ≈ 3.32193 used in the 32-bit version
- Handles the full range of 64-bit unsigned integers (0 to 18,446,744,073,709,551,615)
- Returns values in the range 1-20 (since a 64-bit unsigned int can have at most 20 decimal digits)
- The UINT64CONST macro ensures proper constant handling across different platforms and compilers