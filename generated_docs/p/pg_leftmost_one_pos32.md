# pg_leftmost_one_pos32

## Location
[src/include/port/pg_bitutils.h:41-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_bitutils.h#L41-L71)

## Overview
Returns the position of the most significant set bit in a 32-bit word, providing efficient bit position finding for various PostgreSQL operations.

## Definition

```c
static inline int
pg_leftmost_one_pos32(uint32 word)
```
## Detailed Description
This function finds the position of the leftmost (most significant) set bit in a 32-bit unsigned integer, with positions measured from the least significant bit (0-based indexing). The function provides platform-optimized implementations using compiler builtins when available, falling back to a lookup table approach for maximum portability.

The implementation uses three different strategies based on available platform features:
1. GCC/Clang builtin  for efficient hardware-supported bit counting
2. Microsoft Visual C++  intrinsic for Windows platforms  
3. Fallback implementation using byte-wise scanning with a 256-entry lookup table

## Parameters / Member Variables
- : A 32-bit unsigned integer that must not be zero (the function asserts this precondition)

## Dependencies
- Functions called/Symbols referenced:
  -  (GCC/Clang builtin, when available)
  -  (MSVC intrinsic, when available)
  -  (lookup table for fallback implementation)
- Called from (representative examples):
  -  (hash index initialization)
  -  (HyperLogLog cardinality estimation)
  -  (query planner path generation)
  -  (numeric formatting utilities)
  -  (power-of-2 calculations)
  -  (logarithm calculations)
  -  (bitmapset operations)

## Notes and Other Information
- The function requires that the input word is non-zero, as finding the leftmost bit in zero is undefined
- Returns values from 0 to 31, where 0 indicates the least significant bit is set, and 31 indicates the most significant bit is set
- The fallback implementation processes the word in 8-bit chunks for efficiency, reducing the number of lookup operations required
- This function is heavily used throughout PostgreSQL for bit manipulation, hash table sizing, memory allocation, and query optimization
- The inline declaration ensures minimal function call overhead for this performance-critical utility