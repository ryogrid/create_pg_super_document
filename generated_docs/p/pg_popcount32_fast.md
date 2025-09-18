# pg_popcount32_fast

## Location
src/port/pg_bitutils.c: 215 - 231

## Overview
A fast implementation of 32-bit population count that uses hardware-accelerated instructions to count the number of set bits in a 32-bit word.

## Definition
```c
static inline int pg_popcount32_fast(uint32 word)
```

## Detailed Description
This function provides an optimized implementation for counting the number of 1-bits in a 32-bit unsigned integer. It leverages platform-specific hardware instructions for maximum performance: on Microsoft Visual C++ it uses the __popcnt intrinsic, while on other platforms it uses inline assembly with the popcntl instruction. This function is selected by the dynamic function selection mechanism when hardware support for population count instructions is detected.

## Parameters / Member Variables
- `word`: The 32-bit unsigned integer value for which to count the number of set bits

## Dependencies
- Functions called/Symbols referenced:
  - __popcnt (Microsoft Visual C++ intrinsic)
  - popcntl (x86 assembly instruction)
- Called from (representative examples):
  - choose_popcount_functions
  - pg_popcount_fast
  - pg_popcount_masked_fast

## Notes and Other Information
- Uses conditional compilation for different platforms (MSVC vs GCC/Clang)
- Requires hardware support for POPCNT instruction (SSE4.2 or later)
- Static inline function for optimal performance
- Part of PostgreSQL's runtime-optimized bit manipulation utilities
- Returns int rather than uint32 to match conventional popcount API