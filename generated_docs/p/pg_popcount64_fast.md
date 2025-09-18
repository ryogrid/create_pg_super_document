# pg_popcount64_fast

## Location
src/port/pg_bitutils.c: 232 - 248

## Overview
A fast implementation of 64-bit population count that uses hardware-accelerated instructions to count the number of set bits in a 64-bit word.

## Definition
```c
static inline int pg_popcount64_fast(uint64 word)
```

## Detailed Description
This function provides an optimized implementation for counting the number of 1-bits in a 64-bit unsigned integer. Similar to its 32-bit counterpart, it leverages platform-specific hardware instructions for maximum performance: on Microsoft Visual C++ it uses the __popcnt64 intrinsic, while on other platforms it uses inline assembly with the popcntq (64-bit popcount) instruction. This function is selected by the dynamic function selection mechanism when hardware support for population count instructions is detected.

## Parameters / Member Variables
- `word`: The 64-bit unsigned integer value for which to count the number of set bits

## Dependencies
- Functions called/Symbols referenced:
  - __popcnt64 (Microsoft Visual C++ intrinsic)
  - popcntq (x86-64 assembly instruction)
- Called from (representative examples):
  - choose_popcount_functions
  - pg_popcount_fast
  - pg_popcount_masked_fast

## Notes and Other Information
- Uses conditional compilation for different platforms (MSVC vs GCC/Clang)
- Requires hardware support for POPCNT instruction (SSE4.2 or later)
- Static inline function for optimal performance
- Part of PostgreSQL's runtime-optimized bit manipulation utilities
- Returns int rather than uint64 to match conventional popcount API
- Uses popcntq instruction for 64-bit operands on x86-64 architecture