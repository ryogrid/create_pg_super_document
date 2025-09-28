# pg_nextpower2_32

## Location
[src/include/port/pg_bitutils.h:189-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_bitutils.h#L189-L211)

## Overview
Returns the next higher power of 2 above a given number, or the number itself if it is already a power of 2, optimized for 32-bit values.

## Definition

```c
static inline uint32
pg_nextpower2_32(uint32 num)
```
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

## Simplified Source

```c
// Simplified version of pg_nextpower2_32
static inline uint32
pg_nextpower2_32(uint32 num)
{
    // Input validation: ensure num > 0 and within safe bounds
    Assert(num > 0 && num <= PG_UINT32_MAX / 2 + 1);

    // Check if already a power of 2 using bit trick
    // Power of 2 numbers have only 1 bit set, so (num & (num-1)) == 0
    if ((num & (num - 1)) == 0)
        return num;  // Already a power of 2

    // Find next power of 2: shift 1 left by (position of leftmost bit + 1)
    return ((uint32) 1) << (pg_leftmost_one_pos32(num) + 1);
}
```

Key simplifications made:
- Added clear comments explaining the bit manipulation logic
- Explained the power-of-2 detection technique in plain terms
- Clarified the algorithm for finding the next power of 2
- Maintained all original logic and error checking
- Enhanced readability without changing functionality