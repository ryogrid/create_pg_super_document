# pg_leftmost_one_pos64

## Location
[src/include/port/pg_bitutils.h:72-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_bitutils.h#L72-L110)

## Overview
Returns the position of the most significant set bit in a 64-bit word, providing efficient bit position finding for 64-bit operations in PostgreSQL.

## Definition

```c
static inline int
pg_leftmost_one_pos64(uint64 word)
```
## Detailed Description
This function finds the position of the leftmost (most significant) set bit in a 64-bit unsigned integer, with positions measured from the least significant bit (0-based indexing). Like its 32-bit counterpart, it provides platform-optimized implementations using compiler builtins when available.

The implementation adapts to different 64-bit integer representations:
1. Uses appropriate GCC/Clang builtins ( for long int or  for long long int)
2. Microsoft Visual C++  intrinsic on 64-bit Windows platforms (AMD64/ARM64)
3. Fallback implementation using byte-wise scanning with the same lookup table as the 32-bit version

## Parameters / Member Variables
- `word`: A 64-bit unsigned integer that must not be zero (the function asserts this precondition)
## Dependencies
- Functions called/Symbols referenced:
  -  or  (GCC/Clang builtins, when available)
  -  (MSVC intrinsic, when available)
  -  (lookup table for fallback implementation)
- Called from (representative examples):
  -  (numeric formatting for 64-bit values)
  -  (pgbench random number operations)
  -  (pseudorandom number generation)
  -  (radix tree key operations)
  -  (64-bit power-of-2 calculations)
  -  (64-bit logarithm calculations)
  -  (bitmapset operations for 64-bit words)

## Notes and Other Information
- Returns values from 0 to 63, where 0 indicates the least significant bit is set, and 63 indicates the most significant bit is set
- The function ensures compatibility across different 64-bit integer type definitions (long vs long long)
- Critical for 64-bit memory management, large hash table operations, and high-precision numeric computations
- The MSVC implementation is specifically optimized for 64-bit architectures (x64 and ARM64)
- Used extensively in PostgreSQL's advanced data structures like radix trees and large-scale parallel operations

## Simplified Source

```c
// Simplified version of pg_leftmost_one_pos64
// Finds the position of the leftmost (most significant) set bit in a 64-bit word
static inline int pg_leftmost_one_pos64(uint64 word) {
    // Input validation: word must not be zero
    Assert(word != 0);

    // Method 1: Use GCC/Clang compiler builtin for count leading zeros
    #ifdef HAVE__BUILTIN_CLZ
        #if defined(HAVE_LONG_INT_64)
            // For systems where long is 64-bit, use __builtin_clzl
            return 63 - __builtin_clzl(word);
        #elif defined(HAVE_LONG_LONG_INT_64)
            // For systems where long long is 64-bit, use __builtin_clzll
            return 63 - __builtin_clzll(word);
        #endif

    // Method 2: Use Microsoft Visual C++ intrinsic on 64-bit platforms
    #elif defined(_MSC_VER) && (defined(_M_AMD64) || defined(_M_ARM64))
        unsigned long result;
        // Use MSVC's bit scan reverse intrinsic
        _BitScanReverse64(&result, word);
        return (int) result;

    // Method 3: Fallback implementation using byte-wise scanning
    #else
        int shift = 64 - 8;  // Start from the most significant byte

        // Find the highest byte that contains a set bit
        while ((word >> shift) == 0) {
            shift -= 8;  // Move to next lower byte
        }

        // Use lookup table for the final byte position
        return shift + pg_leftmost_one_pos[(word >> shift) & 255];
    #endif
}
```

Key simplifications made:
- Added clear comments explaining each implementation method
- Simplified conditional compilation logic with descriptive comments
- Removed error directive for clearer focus on main logic paths
- Consolidated variable declarations and made the algorithm flow more obvious
- Emphasized the three distinct implementation strategies based on platform capabilities