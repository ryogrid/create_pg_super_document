# pg_leftmost_one_pos64

## Location
src/include/port/pg_bitutils.h: 72 - 110

## Overview
Returns the position of the most significant set bit in a 64-bit word, providing efficient bit position finding for 64-bit operations in PostgreSQL.

## Definition


## Detailed Description
This function finds the position of the leftmost (most significant) set bit in a 64-bit unsigned integer, with positions measured from the least significant bit (0-based indexing). Like its 32-bit counterpart, it provides platform-optimized implementations using compiler builtins when available.

The implementation adapts to different 64-bit integer representations:
1. Uses appropriate GCC/Clang builtins ( for long int or  for long long int)
2. Microsoft Visual C++  intrinsic on 64-bit Windows platforms (AMD64/ARM64)
3. Fallback implementation using byte-wise scanning with the same lookup table as the 32-bit version

## Parameters / Member Variables
- : A 64-bit unsigned integer that must not be zero (the function asserts this precondition)

## Dependencies
- Functions called/Symbols referenced:
  -  or  (GCC/Clang builtins, when available)
  -  (MSVC intrinsic, when available)
  -  (lookup table for fallback implementation)
- Called from (representative examples):
  -  (numeric formatting for 64-bit values)
  -  (pgbench random number operations)
  -  (pseudorandom number generation)
  -  (radix tree key operations)
  -  (64-bit power-of-2 calculations)
  -  (64-bit logarithm calculations)
  -  (bitmapset operations for 64-bit words)

## Notes and Other Information
- Returns values from 0 to 63, where 0 indicates the least significant bit is set, and 63 indicates the most significant bit is set
- The function ensures compatibility across different 64-bit integer type definitions (long vs long long)
- Critical for 64-bit memory management, large hash table operations, and high-precision numeric computations
- The MSVC implementation is specifically optimized for 64-bit architectures (x64 and ARM64)
- Used extensively in PostgreSQL's advanced data structures like radix trees and large-scale parallel operations