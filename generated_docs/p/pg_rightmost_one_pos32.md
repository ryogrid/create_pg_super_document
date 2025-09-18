# pg_rightmost_one_pos32

## Location
[src/include/port/pg_bitutils.h:111-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_bitutils.h#L111-L144)

## Overview
Returns the position of the least significant set bit in a 32-bit word, providing efficient trailing zero counting for various PostgreSQL bit manipulation operations.

## Definition


## Detailed Description
This function finds the position of the rightmost (least significant) set bit in a 32-bit unsigned integer, with positions measured from the least significant bit (0-based indexing). It essentially counts the number of trailing zeros in the binary representation of the word.

The implementation uses platform-specific optimizations:
1. GCC/Clang builtin  (count trailing zeros) for hardware-accelerated bit counting
2. Microsoft Visual C++  intrinsic for Windows platforms
3. Fallback implementation that processes bytes from right to left using a lookup table

## Parameters / Member Variables
- : A 32-bit unsigned integer that must not be zero (the function asserts this precondition)

## Dependencies
- Functions called/Symbols referenced:
  -  (GCC/Clang builtin, when available)
  -  (MSVC intrinsic, when available)
  -  (lookup table for fallback implementation)
- Called from (representative examples):
  -  (process signaling mechanism)
  -  (radix tree node searching)
  -  (radix tree insertion position finding)
  -  (bitmapset rightmost bit operations)

## Notes and Other Information
- Returns values from 0 to 31, where 0 means the least significant bit is set, and 31 means only the most significant bit is set
- The function is critical for algorithms that need to find the lowest set bit, such as bitmap scanning and tree traversal
- Used less frequently than its leftmost counterpart but essential for specific data structure operations
- The fallback implementation efficiently skips zero bytes by checking 8 bits at a time
- Particularly important for radix tree operations where bit patterns determine navigation paths