# pg_rightmost_one_pos64

## Location
[src/include/port/pg_bitutils.h:145-188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_bitutils.h#L145-L188)

## Overview
Returns the position of the least significant set bit in a 64-bit word, providing efficient trailing zero counting for 64-bit PostgreSQL operations.

## Definition


## Detailed Description
This function finds the position of the rightmost (least significant) set bit in a 64-bit unsigned integer, serving as the 64-bit version of pg_rightmost_one_pos32. It counts trailing zeros in 64-bit values, which is essential for large-scale bit manipulation operations.

The implementation follows the same pattern as other bit utilities:
1. GCC/Clang builtins ( for long or  for long long)
2. Microsoft Visual C++  intrinsic for 64-bit Windows platforms
3. Fallback byte-wise scanning using the same lookup table approach

## Parameters / Member Variables
- : A 64-bit unsigned integer that must not be zero (the function asserts this precondition)

## Dependencies
- Functions called/Symbols referenced:
  -  or  (GCC/Clang builtins, when available)
  -  (MSVC intrinsic, when available)
  -  (lookup table for fallback implementation)
- Called from (representative examples):
  -  (bitmapset operations for 64-bit words)

## Notes and Other Information
- Returns values from 0 to 63, where 0 means the least significant bit is set, and 63 means only the most significant bit is set
- Less commonly used than the 32-bit version, but crucial for operations on large bitmaps and 64-bit data structures
- The MSVC implementation is optimized for 64-bit architectures (AMD64/ARM64)
- Essential for advanced data structure operations that work with large bit patterns
- Provides consistent behavior across different 64-bit integer type definitions