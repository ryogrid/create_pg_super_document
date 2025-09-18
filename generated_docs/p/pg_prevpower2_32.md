# pg_prevpower2_32

## Location
src/include/port/pg_bitutils.h: 235 - 247

## Overview
Returns the next lower power of 2 below the given number, or the number itself if it's already a power of 2.

## Definition
static inline uint32 pg_prevpower2_32(uint32 num)

## Detailed Description
This function efficiently computes the previous (lower) power of 2 for 32-bit unsigned integers. Unlike pg_nextpower2_64, this function works by finding the position of the leftmost (most significant) bit and creating a power of 2 at that position. This effectively gives the largest power of 2 that is less than or equal to the input number.

## Parameters / Member Variables
- num: The input 32-bit unsigned integer for which to find the previous power of 2. Must not be 0.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_leftmost_one_pos32](pg_leftmost_one_pos32.md) (to find the position of the leftmost set bit)
- Called from (representative examples):
  - [ExecParallelHashIncreaseNumBatches](../E/ExecParallelHashIncreaseNumBatches.md) (hash table batch management)
  - pg_prevpower2_size_t (size_t variant wrapper)

## Notes and Other Information
- Much simpler implementation than pg_nextpower2_64 as it only needs to find the leftmost bit position
- Uses bit shifting to create a power of 2 at the leftmost bit position
- If the input is already a power of 2, the result will be the same as the input
- If the input has multiple bits set, the result will be the largest power of 2 that fits within the input
- Commonly used in scenarios where you need to find the largest power-of-2 chunk size that fits within a given limit
- The function assumes num > 0; behavior with num = 0 is undefined