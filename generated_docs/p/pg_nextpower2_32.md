# pg_nextpower2_32

## Location
src/include/port/pg_bitutils.h: 189 - 211

## Overview
Returns the next higher power of 2 above a given number, or the number itself if it is already a power of 2, optimized for 32-bit values.

## Definition


## Detailed Description
This function efficiently computes the smallest power of 2 that is greater than or equal to the input number. It uses a clever bit manipulation technique to detect if a number is already a power of 2, and if not, calculates the next power of 2 using the position of the most significant bit.

The algorithm works as follows:
1. First checks if the input is already a power of 2 using the bit trick 
2. If already a power of 2, returns the input unchanged
3. Otherwise, finds the position of the leftmost set bit and returns 

The function enforces strict bounds checking to prevent overflow, requiring that input values do not exceed .

## Parameters / Member Variables
- : A 32-bit unsigned integer that must be greater than 0 and not exceed  (the function asserts these preconditions)

## Dependencies
- Functions called/Symbols referenced:
  -  (to find the position of the most significant bit)
  -  (constant for maximum 32-bit unsigned integer value)
- Called from (representative examples):
  -  (GIN index tuple collection)
  -  (hash index initialization)
  -  (hash table spooling)
  -  (hash join table sizing)
  -  and  (dynamic list management)
  -  (lock management)
  -  (array aggregation functions)
  - Memory allocators and cache management functions

## Notes and Other Information
- Critical for hash table sizing, memory allocation, and data structure initialization throughout PostgreSQL
- The power-of-2 constraint is essential for efficient modulo operations using bitwise AND
- Used extensively in the executor for hash join operations where table sizes must be powers of 2
- The bounds checking prevents integer overflow that could lead to incorrect results or security issues
- The bit manipulation technique  is a well-known method to test if a number is a power of 2
- Performance-critical function that appears in many hot code paths, hence the inline implementation