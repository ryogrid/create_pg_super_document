# my_log2

## Location
[src/backend/utils/hash/dynahash.c:1754-1771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1754-L1771)

## Overview
Calculates the ceiling of the base-2 logarithm of a given number, with platform-specific optimizations.

## Definition

```c
int
my_log2(long num)
```
## Detailed Description
This function computes the ceiling of the logarithm base 2 of the input number, which is equivalent to finding the minimum number of bits required to represent values from 0 to num-1. The function includes protection against excessively large inputs by capping the input at LONG_MAX/2 to prevent overflow in the underlying calculation functions.

The implementation is platform-aware, using different PostgreSQL utility functions based on the size of the long data type. On 32-bit platforms (where SIZEOF_LONG < 8), it uses pg_ceil_log2_32(), while on 64-bit platforms it uses pg_ceil_log2_64(). This ensures optimal performance and correctness across different architectures.

## Parameters / Member Variables
- `num`: The input number for which to calculate ceil(log₂(num))
## Dependencies
- Functions called/Symbols referenced:
  - [pg_ceil_log2_32](../p/pg_ceil_log2_32.md) (32-bit ceiling log2 calculation)
  - [pg_ceil_log2_64](../p/pg_ceil_log2_64.md) (64-bit ceiling log2 calculation)
  - LONG_MAX (maximum value for long type)
  - SIZEOF_LONG (compile-time size of long type)
- Called from (representative examples):
  - [hash_choose_num_partitions](../h/hash_choose_num_partitions.md)
  - [ExecHashTableCreate](../E/ExecHashTableCreate.md)
  - [hash_create](../h/hash_create.md)
  - [next_pow2_long](../n/next_pow2_long.md)
  - [next_pow2_int](../n/next_pow2_int.md)

## Notes and Other Information
- Returns an integer representing the number of bits needed
- Input validation prevents overflow by capping at LONG_MAX/2
- Platform-specific implementation ensures optimal performance
- Commonly used in hash table sizing calculations and memory allocation
- Essential for determining appropriate power-of-2 sizes for data structures
- The ceiling operation ensures the result is always sufficient to represent the full range [0, num-1]

## Simplified Source

```c
// Simplified version of my_log2
// Calculates ceiling of log base 2 of a number
int my_log2(long num) {
    // Guard against overflow: cap input at safe maximum
    if (num > LONG_MAX / 2) {
        num = LONG_MAX / 2;
    }

    // Use platform-appropriate calculation function
    if (sizeof(long) < 8) {
        // 32-bit platform: use 32-bit optimized version
        return pg_ceil_log2_32(num);
    } else {
        // 64-bit platform: use 64-bit optimized version
        return pg_ceil_log2_64(num);
    }
}
```

Key simplifications made:
- Added clear comments explaining each step
- Replaced compile-time macro SIZEOF_LONG with sizeof() for clarity
- Made the platform selection logic more explicit
- Focused on the main execution path with clearer variable handling
- Preserved the essential overflow protection and platform-specific optimization