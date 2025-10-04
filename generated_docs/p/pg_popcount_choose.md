# pg_popcount_choose

## Location
[src/port/pg_bitutils.c:197-203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_bitutils.c#L197-L203)

## Overview
Initial chooser function for bulk popcount operations on byte arrays that triggers runtime CPU feature detection and then delegates to the selected optimal implementation.

## Definition
```c
static uint64 pg_popcount_choose(const char *buf, int bytes)
```

## Detailed Description
This function serves as the initial entry point for bulk popcount operations on byte arrays before CPU capabilities have been detected. It implements a lazy initialization pattern where the first call triggers choose_popcount_functions() to detect CPU features and reassign the global pg_popcount_optimized function pointer to the optimal implementation.

The function is designed for counting set bits across multiple bytes efficiently, which is particularly useful for bitmap operations and bit vector processing in PostgreSQL. After calling choose_popcount_functions(), it immediately delegates to the newly selected pg_popcount_optimized implementation to complete the actual bulk popcount operation.

This chooser function enables PostgreSQL to adaptively select between different implementation tiers: portable C code, POPCNT-optimized assembly, or AVX-512 VPOPCNT instructions depending on CPU capabilities.

## Parameters / Member Variables
- `buf`: const char* - Pointer to the byte array to process
- `bytes`: int - Number of bytes in the array to process
- Returns: uint64 - The total number of set bits across all bytes in the buffer

## Dependencies
- Functions called/Symbols referenced:
  - [choose_popcount_functions](../c/choose_popcount_functions.md)
  - [pg_popcount_optimized](pg_popcount_optimized.md) (global function pointer)
- Called from:
  - No direct references found (likely assigned to function pointer during initialization)

## Notes and Other Information
- This is a static function, only accessible within pg_bitutils.c
- Part of PostgreSQL's runtime optimization system for bulk bit manipulation operations
- Used only during the first invocation of bulk popcount operations
- Designed for processing larger data structures like bitmaps and bit vectors
- After initialization, this function is no longer called as the global pointer is redirected
- The bulk processing nature makes CPU optimization particularly impactful for performance
- May benefit significantly from AVX-512 VPOPCNT instructions on supported hardware

## Simplified Source

```c
static uint64 pg_popcount_choose(const char *buf, int bytes) {
    // Initialize function selection mechanism
    choose_popcount_functions();

    // Delegate to selected bulk popcount implementation
    return pg_popcount_optimized(buf, bytes);
}
```