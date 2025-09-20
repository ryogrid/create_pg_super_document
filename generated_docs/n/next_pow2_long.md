# next_pow2_long

## Location
[src/backend/utils/hash/dynahash.c:1772-1779](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1772-L1779)

## Overview
Calculates the smallest power of 2 that is greater than or equal to the given number, bounded by long integer limits.

## Definition

```c
static long
next_pow2_long(long num)
```
## Detailed Description
This function computes the next power of 2 for a given input by leveraging the my_log2 function to determine the appropriate exponent and then using bit shifting to calculate the result. The implementation is both efficient and safe, as it relies on my_log2's built-in range checking to prevent overflow conditions.

The function uses the mathematical relationship that the smallest power of 2 greater than or equal to num is 2^(ceil(log₂(num))). By using bit shifting (1L << exponent), it achieves this calculation efficiently while ensuring the result fits within the bounds of a long integer.

## Parameters / Member Variables
- : The input number for which to find the next power of 2

## Dependencies
- Functions called/Symbols referenced:
  - [my_log2](../m/my_log2.md) (calculates ceiling of base-2 logarithm)
  - Bit shift operator (<<) for power-of-2 calculation
- Called from (representative examples):
  - [hash_estimate_size](../h/hash_estimate_size.md)
  - [hash_select_dirsize](../h/hash_select_dirsize.md)

## Notes and Other Information
- Returns a long integer representing the next power of 2
- Relies on my_log2's range validation, eliminating the need for additional bounds checking
- Uses bit shifting for optimal performance (1L << n is equivalent to 2^n)
- Essential for hash table sizing where power-of-2 dimensions are required for efficient modular arithmetic
- The 'L' suffix ensures long integer arithmetic is used throughout the calculation
- Static function, indicating it's only used within the dynahash.c module
- Commonly used in hash table initialization and resizing operations