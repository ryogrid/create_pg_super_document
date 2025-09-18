# pg_popcount64_choose

## Location
src/port/pg_bitutils.c: 190 - 196

## Overview
Initial chooser function for 64-bit popcount operations that triggers runtime CPU feature detection and then delegates to the selected optimal implementation.

## Definition
```c
static int pg_popcount64_choose(uint64 word)
```

## Detailed Description
This function serves as the initial entry point for 64-bit popcount operations before CPU capabilities have been detected. It implements a lazy initialization pattern where the first call triggers choose_popcount_functions() to detect CPU features and reassign the global pg_popcount64 function pointer to the optimal implementation (either fast assembly or slow portable C code).

After calling choose_popcount_functions(), it immediately delegates to the newly selected pg_popcount64 implementation to complete the actual popcount operation. Subsequent calls will bypass this chooser function entirely since the global function pointer will have been redirected.

## Parameters / Member Variables
- `word`: uint64 - The 64-bit value for which to count set bits
- Returns: int - The number of set bits in the input word

## Dependencies
- Functions called/Symbols referenced:
  - [choose_popcount_functions](../c/choose_popcount_functions.md)
  - [pg_popcount64](pg_popcount64.md) (global function pointer)
- Called from:
  - No direct references found (likely assigned to function pointer during initialization)

## Notes and Other Information
- This is a static function, only accessible within pg_bitutils.c
- Part of PostgreSQL's runtime optimization system for bit manipulation
- Used only during the first invocation of 64-bit popcount operations
- The function pointer indirection adds minimal overhead while enabling significant performance gains
- After initialization, this function is no longer called as the global pointer is redirected
- Mirrors the functionality of pg_popcount32_choose but operates on 64-bit values