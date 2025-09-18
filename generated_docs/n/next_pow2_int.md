# next_pow2_int

## Location
src/backend/utils/hash/dynahash.c: 1780 - 1815

## Overview
Calculates the first power of 2 that is greater than or equal to a given number, with bounds checking to ensure the result fits within an integer.

## Definition
```c
static int next_pow2_int(long num)
```

## Detailed Description
This utility function computes the smallest power of 2 that is greater than or equal to the input value. It includes safety bounds checking to prevent integer overflow by capping the input at INT_MAX/2 before performing the power-of-2 calculation. The function is used internally within PostgreSQL's hash table implementation to determine appropriate hash table sizes that are powers of 2, which is optimal for hash bucket distribution and bitwise operations.

## Parameters / Member Variables
- `num`: The input value for which to find the next power of 2. This is a long integer that will be bounded to prevent overflow.

## Dependencies
- Functions called/Symbols referenced:
  - my_log2
- Called from (representative examples):
  - MOD
  - hash_create
  - init_htab

## Notes and Other Information
- This is a static function, meaning it's only accessible within the dynahash.c file
- The function ensures thread safety by using only local variables and calling safe utility functions
- Powers of 2 are particularly important in hash table implementations for efficient modulo operations using bitwise AND
- The INT_MAX/2 bound prevents overflow when the result is computed using bit shifting (1 << result)