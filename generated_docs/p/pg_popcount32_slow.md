# pg_popcount32_slow

## Location
[src/port/pg_bitutils.c:348-369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_bitutils.c#L348-L369)

## Overview
A fallback implementation for counting the number of 1 bits in a 32-bit unsigned integer, used when fast hardware instructions are not available.

## Definition

```c
static inline int
pg_popcount32_slow(uint32 word)
```
## Detailed Description
This function provides a portable implementation for population count (popcount) operations on 32-bit values. It serves as a fallback when hardware-optimized popcount instructions are unavailable. The function uses two different approaches based on compiler support:

1. **With built-in support**: Uses  when available (GCC/Clang)
2. **Manual implementation**: When built-ins are not available, it processes the word byte-by-byte using a lookup table () that contains precomputed popcount values for all possible byte values (0-255)

The manual approach works by:
- Processing the input word 8 bits at a time
- Looking up the popcount for each byte in the  array
- Accumulating the total count
- Right-shifting the word by 8 bits for the next iteration

## Parameters / Member Variables
- `word`: The 32-bit unsigned integer for which to count the number of set bits
## Dependencies
- Functions called/Symbols referenced:
  -  (when HAVE__BUILTIN_POPCOUNT is defined)
  -  (lookup table for byte popcount values)
- Called from (representative examples):
  - 
  - 
  - 
  - 

## Notes and Other Information
- This function is marked as  for performance optimization
- It automatically selects the best available implementation at compile time
- The lookup table approach, while slower than hardware instructions, is still more efficient than bit-by-bit counting
- This function is part of PostgreSQL's bit utilities infrastructure and serves as the foundation for other popcount operations

## Simplified Source

```c
static inline int pg_popcount32_slow(uint32 word) {
#ifdef HAVE__BUILTIN_POPCOUNT
    // Use compiler built-in if available
    return __builtin_popcount(word);
#else
    // Manual implementation using lookup table
    int result = 0;

    while (word != 0) {
        // Count bits in each byte using lookup table
        result += pg_number_of_ones[word & 255];
        word >>= 8;
    }

    return result;
#endif
}
```