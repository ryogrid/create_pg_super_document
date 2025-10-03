# pg_popcount64_slow

## Location
[src/port/pg_bitutils.c:370-397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_bitutils.c#L370-L397)

## Overview
A fallback implementation for counting the number of 1 bits in a 64-bit unsigned integer, used when fast hardware instructions are not available.

## Definition

```c
static inline int
pg_popcount64_slow(uint64 word)
```
## Detailed Description
This function provides a portable implementation for population count (popcount) operations on 64-bit values. It serves as a fallback when hardware-optimized popcount instructions are unavailable. The function uses two different approaches based on compiler and platform support:

1. **With built-in support**: Uses compiler built-in functions when available:
   -  on platforms where  is 64-bit
   -  on platforms where  is 64-bit
   - Compilation fails if no 64-bit integer datatype is available

2. **Manual implementation**: When built-ins are not available, it processes the word byte-by-byte using the same approach as the 32-bit version, using the  lookup table

The manual approach processes all 8 bytes of the 64-bit word sequentially, accumulating the popcount for each byte until the entire word is processed.

## Parameters / Member Variables
- `word`: The 64-bit unsigned integer for which to count the number of set bits
## Dependencies
- Functions called/Symbols referenced:
  -  (when HAVE_LONG_INT_64 is defined)
  -  (when HAVE_LONG_LONG_INT_64 is defined)
  -  (lookup table for byte popcount values)
- Called from (representative examples):
  - 
  - 
  - 
  - 

## Notes and Other Information
- This function is marked as  for performance optimization
- It automatically selects the best available implementation at compile time based on platform characteristics
- The function includes compile-time checks to ensure 64-bit integer support exists
- Similar to the 32-bit version but handles twice the data, making the manual implementation correspondingly slower
- Part of PostgreSQL's comprehensive bit manipulation utilities

## Simplified Source

```c
static inline int pg_popcount64_slow(uint64 word) {
    // Use compiler builtin if available
    #ifdef HAVE__BUILTIN_POPCOUNT
        #if defined(HAVE_LONG_INT_64)
            return __builtin_popcountl(word);
        #elif defined(HAVE_LONG_LONG_INT_64)
            return __builtin_popcountll(word);
        #endif
    #else
        // Manual byte-by-byte counting using lookup table
        int result = 0;
        while (word != 0) {
            result += pg_number_of_ones[word & 255];  // Count bits in lower 8 bits
            word >>= 8;                               // Shift to next byte
        }
        return result;
    #endif
}
```