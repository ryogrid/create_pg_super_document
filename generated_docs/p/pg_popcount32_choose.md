# pg_popcount32_choose

## Location
[src/port/pg_bitutils.c:183-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_bitutils.c#L183-L189)

## Overview
Initial chooser function for 32-bit popcount operations that triggers runtime CPU feature detection and then delegates to the selected optimal implementation.

## Definition
```c
static int pg_popcount32_choose(uint32 word)
```

## Detailed Description
This function serves as the initial entry point for 32-bit popcount operations before CPU capabilities have been detected. It implements a lazy initialization pattern where the first call triggers choose_popcount_functions() to detect CPU features and reassign the global pg_popcount32 function pointer to the optimal implementation (either fast assembly or slow portable C code).

After calling choose_popcount_functions(), it immediately delegates to the newly selected pg_popcount32 implementation to complete the actual popcount operation. Subsequent calls will bypass this chooser function entirely since the global function pointer will have been redirected.

## Parameters / Member Variables
- `word`: uint32 - The 32-bit value for which to count set bits
- Returns: int - The number of set bits in the input word

## Dependencies
- Functions called/Symbols referenced:
  - [choose_popcount_functions](../c/choose_popcount_functions.md)
  - [pg_popcount32](pg_popcount32.md) (global function pointer)
- Called from:
  - No direct references found (likely assigned to function pointer during initialization)

## Notes and Other Information
- This is a static function, only accessible within pg_bitutils.c
- Part of PostgreSQL's runtime optimization system for bit manipulation
- Used only during the first invocation of 32-bit popcount operations
- The function pointer indirection adds minimal overhead while enabling significant performance gains
- After initialization, this function is no longer called as the global pointer is redirected

## Simplified Source

```c
static int pg_popcount32_choose(uint32 word) {
    // Trigger runtime CPU feature detection and function pointer selection
    choose_popcount_functions();

    // Delegate to the newly selected optimal implementation
    return pg_popcount32(word);
}
```