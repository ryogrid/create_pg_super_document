# pg_ceil_log2_64

## Location
[src/include/port/pg_bitutils.h:271-285](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_bitutils.h#L271-L285)

## Overview
Returns the equivalent of ceil(log2(num)), providing the ceiling of the base-2 logarithm for 64-bit unsigned integers.

## Definition
static inline uint64 pg_ceil_log2_64(uint64 num)

## Detailed Description
This function efficiently computes the ceiling of the base-2 logarithm for 64-bit unsigned integers without using floating-point arithmetic. It is the 64-bit version of pg_ceil_log2_32 and works identically but handles larger values. The function determines the minimum number of bits required to represent values up to and including the input number, using the same clever bit manipulation technique.

## Parameters / Member Variables
- num: The input 64-bit unsigned integer for which to calculate ceil(log2(num)). Special case: returns 0 for num < 2.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_leftmost_one_pos64](pg_leftmost_one_pos64.md) (to find the position of the leftmost set bit)
- Called from (representative examples):
  - [GetHugePageSize](../G/GetHugePageSize.md) (memory management and huge page calculations)
  - [my_log2](../m/my_log2.md) (dynamic hash table implementation for large sizes)

## Notes and Other Information
- 64-bit version of pg_ceil_log2_32 with identical logic but wider data type
- Returns 0 for inputs less than 2 (including 0 and 1)
- For powers of 2, returns the exact log2 value
- For non-powers of 2, returns the ceiling (next higher integer) of log2
- The (num-1) trick ensures proper ceiling behavior for both powers of 2 and other values
- Handles the full 64-bit range, making it suitable for large memory calculations and huge page sizing
- Much more efficient than using floating-point logarithm functions
- Commonly used in memory management where large block sizes require bit count calculations
- Examples: ceil(log2(2^32)) = 32, ceil(log2(2^32 + 1)) = 33

## Simplified Source

```c
// Simplified version of pg_ceil_log2_64
// Returns the ceiling of log2(num) - the minimum number of bits needed
static inline uint64 pg_ceil_log2_64(uint64 num) {
    // Special case: log2 of 0 or 1 is 0
    if (num < 2) {
        return 0;
    }

    // Core algorithm: find leftmost bit position of (num-1) and add 1
    // The (num-1) trick handles both powers of 2 and non-powers correctly
    // For powers of 2: leftmost_bit(2^n - 1) + 1 = (n-1) + 1 = n
    // For others: leftmost_bit(num-1) + 1 gives the ceiling
    return pg_leftmost_one_pos64(num - 1) + 1;
}
```

Key simplifications made:
- Added detailed comments explaining the algorithm logic
- Clarified the special case handling
- Explained why the (num-1) trick works for both powers of 2 and other values
- Made the core algorithm more readable with explanatory comments