# pg_popcount64

## Location
[src/port/pg_bitutils.c:505-514](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_bitutils.c#L505-L514)

## Overview
Counts the number of 1 bits set in a 64-bit unsigned integer word, providing a high-level interface for population count operations.

## Definition
```c
int pg_popcount64(uint64 word)
```

## Detailed Description
pg_popcount64 is a wrapper function that provides a consistent interface for counting the number of set bits (population count) in a 64-bit unsigned integer. The function delegates to pg_popcount64_slow, which contains the actual implementation logic. This design allows for potential runtime optimization where different implementations can be selected based on CPU capabilities through function pointers when TRY_POPCNT_FAST is enabled.

The function is part of PostgreSQL's bit manipulation utilities and is used throughout the codebase for efficient bit counting operations, particularly in bitmap operations and other data structures that require population counting.

## Parameters / Member Variables
- `word`: A 64-bit unsigned integer whose set bits are to be counted

## Dependencies
- Functions called/Symbols referenced:
  - [pg_popcount64_slow](pg_popcount64_slow.md)
- Called from (representative examples):
  - bmw_popcount (via function pointer in bitmapset operations)
  - TRY_POPCNT_FAST macro (when runtime optimization is enabled)
  - [choose_popcount_functions](../c/choose_popcount_functions.md) (during function pointer initialization)
  - [pg_popcount64_choose](pg_popcount64_choose.md) (part of the dynamic selection mechanism)

## Notes and Other Information
- When TRY_POPCNT_FAST is defined, this function becomes a function pointer that can be dynamically assigned to optimized implementations
- The actual bit counting is performed by pg_popcount64_slow, which uses compiler intrinsics (__builtin_popcountl or __builtin_popcountll) when available, falling back to a lookup table approach otherwise
- This function is part of PostgreSQL's portable bit manipulation library in src/port/pg_bitutils.c
- The function is declared in src/include/port/pg_bitutils.h and is available across the entire PostgreSQL codebase