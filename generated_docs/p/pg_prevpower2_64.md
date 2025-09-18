# pg_prevpower2_64

## Location
src/include/port/pg_bitutils.h: 248 - 257

## Overview
Returns the next lower power of 2 below the given number, or the number itself if it's already a power of 2.

## Definition
static inline uint64 pg_prevpower2_64(uint64 num)

## Detailed Description
This function efficiently computes the previous (lower) power of 2 for 64-bit unsigned integers. It works identically to pg_prevpower2_32 but operates on 64-bit values. The function finds the position of the leftmost (most significant) bit and creates a power of 2 at that position, effectively giving the largest power of 2 that is less than or equal to the input number.

## Parameters / Member Variables
- num: The input 64-bit unsigned integer for which to find the previous power of 2. Must not be 0.

## Dependencies
- Functions called/Symbols referenced:
  - pg_leftmost_one_pos64 (to find the position of the leftmost set bit)
- Called from (representative examples):
  - pg_prevpower2_size_t (size_t variant wrapper)

## Notes and Other Information
- 64-bit version of pg_prevpower2_32 with identical logic but wider data type
- Simple implementation that only needs to find the leftmost bit position
- Uses bit shifting to create a power of 2 at the leftmost bit position
- If the input is already a power of 2, the result will be the same as the input
- If the input has multiple bits set, the result will be the largest power of 2 that fits within the input
- Commonly used in scenarios where you need to find the largest power-of-2 chunk size that fits within a given 64-bit limit
- The function assumes num > 0; behavior with num = 0 is undefined
- Handles the full 64-bit range, making it suitable for large memory calculations and hash table sizing