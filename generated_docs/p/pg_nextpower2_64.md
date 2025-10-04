# pg_nextpower2_64

## Location
[src/include/port/pg_bitutils.h:212-234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_bitutils.h#L212-L234)

## Overview
Returns the next higher power of 2 above the given number, or the number itself if it's already a power of 2.

## Definition
static inline uint64 pg_nextpower2_64(uint64 num)

## Detailed Description
This function efficiently computes the next power of 2 for 64-bit unsigned integers. It uses bit manipulation techniques to determine if the input is already a power of 2, and if not, calculates the next higher power of 2. The implementation leverages the mathematical property that a power of 2 has only one bit set, making the bitwise AND operation between the number and its predecessor equal to zero.

## Parameters / Member Variables
- num: The input 64-bit unsigned integer for which to find the next power of 2. Must be greater than 0 and not exceed PG_UINT64_MAX / 2 + 1.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_leftmost_one_pos64](pg_leftmost_one_pos64.md) (to find the position of the leftmost set bit)
  - PG_UINT64_MAX (maximum value constant for bounds checking)
- Called from (representative examples):
  - [SH_COMPUTE_SIZE](../S/SH_COMPUTE_SIZE.md) (hash table size computation)
  - [SH_GROW](../S/SH_GROW.md) (hash table growth operations)
  - pg_nextpower2_size_t (size_t variant wrapper)

## Notes and Other Information
- The function includes an assertion to ensure the input is within valid bounds
- Uses efficient bit manipulation: checks if (num & (num - 1)) == 0 to detect existing powers of 2
- For non-power-of-2 inputs, shifts 1 left by (leftmost bit position + 1) to get the next power of 2
- Commonly used in hash table implementations and memory allocation routines where power-of-2 sizes are preferred
- The upper bound restriction prevents integer overflow in the result

## Simplified Source

```c
static inline uint64 pg_nextpower2_64(uint64 num) {
    Assert(num > 0 && num <= PG_UINT64_MAX / 2 + 1);

    // Check if already a power of 2
    if ((num & (num - 1)) == 0) {
        return num;
    }

    // Find next higher power of 2
    return ((uint64)1) << (pg_leftmost_one_pos64(num) + 1);
}
```