# pg_ceil_log2_32

## Location
src/include/port/pg_bitutils.h: 258 - 270

## Overview
Returns the equivalent of ceil(log2(num)), providing the ceiling of the base-2 logarithm for 32-bit unsigned integers.

## Definition
static inline uint32 pg_ceil_log2_32(uint32 num)

## Detailed Description
This function efficiently computes the ceiling of the base-2 logarithm for 32-bit unsigned integers without using floating-point arithmetic. It determines the minimum number of bits required to represent values up to and including the input number. The implementation uses a clever trick: for non-powers of 2, it finds the leftmost bit position of (num-1) and adds 1, which gives the ceiling effect.

## Parameters / Member Variables
- num: The input 32-bit unsigned integer for which to calculate ceil(log2(num)). Special case: returns 0 for num < 2.

## Dependencies
- Functions called/Symbols referenced:
  - pg_leftmost_one_pos32 (to find the position of the leftmost set bit)
- Called from (representative examples):
  - _hash_spareindex (hash table index calculations)
  - my_log2 (dynamic hash table implementation)

## Notes and Other Information
- Returns 0 for inputs less than 2 (including 0 and 1)
- For powers of 2, returns the exact log2 value
- For non-powers of 2, returns the ceiling (next higher integer) of log2
- The trick of using (num-1) ensures proper ceiling behavior: for exact powers of 2, subtracting 1 moves to the previous bit position, and adding 1 back gives the correct result
- Commonly used in hash table implementations to determine the number of bits needed for indexing
- Much more efficient than using floating-point logarithm functions
- Examples: ceil(log2(8)) = 3, ceil(log2(9)) = 4, ceil(log2(16)) = 4, ceil(log2(17)) = 5